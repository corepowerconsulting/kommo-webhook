from flask import Flask, request, jsonify, render_template, session, redirect, url_for
import bisect
import html
import json
import re
import secrets
import time
import threading
from datetime import datetime, timedelta
from functools import wraps
import calendar
from collections import defaultdict, Counter
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from pulse_config import PULSE_CONFIG, ASESORES, REMITENTES_AUTOMATICOS
# Todos los colores salen de paleta.py, con el porque de cada eleccion escrito
# ahi. Se importan con alias porque en este archivo OTROS ya es una etiqueta de
# texto y ASESORES ya es la lista de personas.
import paleta
from paleta import ASESORES as PALETA_ASESORES, OTROS as COLOR_OTROS

# Nombres de embudos y etapas, bajados de la API con scripts/bajar_embudos.py.
# El webhook manda numeros (pipeline_id 7008155, status_id 74938892) y los
# nombres solo estan en la API.
#
# Se lee de un archivo y no se consulta en vivo: los embudos casi no cambian, y
# depender de la API en cada carga del dashboard agregaria una llamada de red
# al camino critico y obligaria a tener un token de larga duracion en
# produccion. Es el mismo criterio que ASESORES.
#
# Si el archivo no esta, el dashboard sigue funcionando y muestra los numeros.
def _cargar_embudos():
    ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'embudos.json')
    try:
        with open(ruta, encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print('⚠️  Sin embudos.json: los embudos se muestran por numero. '
              'Correr scripts/bajar_embudos.py <cuenta> --guardar')
        return {}
    except ValueError as e:
        print(f'⚠️  embudos.json ilegible ({e}); se muestran los numeros.')
        return {}

EMBUDOS = _cargar_embudos()

def nombre_embudo(subdomain, pipeline_id):
    if pipeline_id in (None, ''):
        return None
    emb = (EMBUDOS.get(subdomain, {}).get('embudos') or {}).get(str(pipeline_id))
    return (emb or {}).get('nombre') or f'Embudo {pipeline_id}'

def nombre_etapa(subdomain, pipeline_id, status_id):
    """El nombre de la etapa. Hace falta el embudo: los ids de etapa son unicos
    por cuenta salvo 142 y 143, que Kommo reserva para ganado y perdido y por
    eso se repiten en TODOS los embudos con nombres distintos ('Leads ganados',
    'CITAS PROGRAMADAS', 'VENDEDOR CREADO'...). Buscar 142 sin saber el embudo
    devolveria el nombre de otro."""
    if status_id in (None, ''):
        return None
    emb = (EMBUDOS.get(subdomain, {}).get('embudos') or {}).get(str(pipeline_id)) or {}
    return (emb.get('etapas') or {}).get(str(status_id)) or f'Etapa {status_id}'

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no está configurada")

app.secret_key = os.environ.get('SECRET_KEY')
if not app.secret_key:
    app.secret_key = secrets.token_hex(32)
    print("⚠️  SECRET_KEY no configurada: usando una temporal. "
          "Los usuarios de PULSE se desloguean en cada deploy/restart. "
          "Configura SECRET_KEY en Render para que las sesiones persistan.")

def pulse_password(subdomain):
    return os.environ.get(f'PULSE_PW_{subdomain.upper()}')

def pulse_autorizado(subdomain):
    return bool(session.get(f'pulse_ok_{subdomain}'))

def _token_admin_ok():
    """El ADMIN_TOKEN tambien abre /pulse/data, ademas de la sesion.

    Para medir cuanto tarda el tablero desde afuera —scripts, el perfil de
    tiempos— sin tener que loguearse con la clave de cada cliente. El token
    ya abre /data y los backfills, que exponen mas que esto."""
    entregado = request.args.get('token', '')
    return bool(ADMIN_TOKEN) and bool(entregado) and secrets.compare_digest(entregado, ADMIN_TOKEN)

# Contadores de carga en memoria, para /health/carga. Se reinician con cada
# deploy; alcanza, porque lo que se busca es el pico durante una rafaga.
_CARGA = {
    'webhooks': 0,
    'hilos_maximo': 0,
    'cuando_el_maximo': None,
    'arranque': datetime.utcnow().isoformat(),
    # Las dos formas conocidas en que este endpoint puede fallarle a Kommo.
    # Kommo dice textualmente "Webhook esta desactivado debido a una respuesta
    # no valida", asi que cualquier cosa que no sea 200 lo desactiva. Si alguno
    # de estos sube de 0, ahi esta la causa de las desactivaciones.
    'hilos_rechazados': 0,
    'errores_body': 0,
    # Escrituras a la base. El 31/08/2026 Supabase entro en modo solo lectura y
    # estuvimos 43 horas recibiendo webhooks sin guardar uno solo. Nadie se
    # entero: cada fallo hacia un print y seguia, y este endpoint miraba hilos
    # y errores de body —los dos en cero— asi que informaba que todo iba bien.
    # El tablero se vacio en silencio y lo noto el cliente antes que nosotros.
    #
    # Contar los fallos es lo que habria avisado el mismo dia.
    'escrituras_ok': 0,
    'escrituras_fallidas': 0,
    'ultimo_error_escritura': None,
    'cuando_el_ultimo_error': None,
    # Eventos que llegaron y se decidio NO guardar (ver _que_guardar). Se
    # cuentan para que el ahorro sea visible y no un descarte a ciegas.
    'eventos_no_guardados': 0,
}

def _fallo_escritura(donde, e):
    """Una escritura a la base fallo: se CUENTA, no solo se imprime.

    'donde' nombra la tabla, para que el contador diga si falla todo o solo
    una. El mensaje de Postgres se guarda entero porque es el que da el
    diagnostico en una linea: 'cannot execute INSERT in a read-only
    transaction' fue el del 31/08, y tardamos dos dias en leerlo porque solo
    vivia en los logs de Render."""
    _CARGA['escrituras_fallidas'] += 1
    _CARGA['ultimo_error_escritura'] = f'{donde}: {e}'
    _CARGA['cuando_el_ultimo_error'] = datetime.utcnow().isoformat()
    print(f"❌ Error {donde}: {e}")

ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN')
if not ADMIN_TOKEN:
    print("⚠️  ADMIN_TOKEN no configurada: /data, /respuestas y /backfill* "
          "responden 401. Configurala en Render para poder usarlos.")

def token_requerido(f):
    """Cierra los endpoints que devuelven datos de clientes o escriben en la base.

    Estaban abiertos: cualquiera con el link leia en /data los payloads crudos
    de Kommo — telefonos, nombres y texto de los mensajes de las seis cuentas —
    y podia disparar /backfill, que ESCRIBE y ademas es un GET, o sea que
    alcanzaba con que un crawler lo visitara.

    Es la misma exposicion que Supabase reporto como rls_disabled_in_public,
    por HTTP en vez de por la API REST.

    Sin ADMIN_TOKEN configurada se responde 401. Es preferible que estos
    endpoints dejen de funcionar a que queden abiertos por olvidar la variable:
    el fallo se nota enseguida, la exposicion no."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        entregado = request.args.get('token', '')
        # compare_digest y no ==: la comparacion normal corta en la primera
        # letra distinta, y esa diferencia de tiempo permite adivinar el token
        # caracter por caracter.
        if not ADMIN_TOKEN or not secrets.compare_digest(entregado, ADMIN_TOKEN):
            return jsonify({'error': 'no autorizado'}), 401
        return f(*args, **kwargs)
    return wrapper

# ========================
# BASE DE DATOS
# ========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def _agregar_columna(c, tabla, columna, tipo):
    """ALTER TABLE ... ADD COLUMN solo si la columna NO existe.

    'ADD COLUMN IF NOT EXISTS' parece inofensivo, pero toma un bloqueo
    exclusivo sobre la tabla AUNQUE la columna ya este. init_db lo hacia
    nueve veces en cada arranque, y en un deploy el proceso nuevo arranca
    mientras el viejo sigue sirviendo el tablero: el 04/09 se cruzaron y
    Postgres mato una consulta con "deadlock detected". Preguntar primero al
    catalogo no bloquea nada."""
    c.execute(
        'SELECT 1 FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s AND column_name = %s',
        ('public', tabla, columna))
    if c.fetchone() is None:
        c.execute(f'ALTER TABLE {tabla} ADD COLUMN IF NOT EXISTS {columna} {tipo}')

def _activar_rls(c, tabla):
    """ENABLE ROW LEVEL SECURITY solo si no esta activo. Misma razon que
    _agregar_columna: el ALTER bloquea la tabla aunque no cambie nada."""
    c.execute('SELECT relrowsecurity FROM pg_class WHERE relname = %s', (tabla,))
    fila = c.fetchone()
    if fila is not None and not fila[0]:
        c.execute(f'ALTER TABLE {tabla} ENABLE ROW LEVEL SECURITY')

def init_db():
    conn = get_conn()
    # Cada sentencia en su propia transaccion. Antes iban todas en una y los
    # bloqueos de cada ALTER se acumulaban sobre todas las tablas hasta el
    # commit final: es la mitad del deadlock del 04/09 (la otra mitad era el
    # tablero reteniendo sus bloqueos de lectura, ver pulse_data).
    conn.autocommit = True
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS eventos (
            id SERIAL PRIMARY KEY,
            subdomain TEXT,
            tipo_evento TEXT,
            lead_id TEXT,
            timestamp BIGINT,
            raw_data TEXT,
            capturado_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS tiempos_respuesta (
            id SERIAL PRIMARY KEY,
            subdomain TEXT,
            lead_id TEXT,
            f_ult_msj_cliente BIGINT,
            f_ult_msj_asesor BIGINT,
            tiempo_respuesta_seg INTEGER,
            capturado_at TEXT,
            UNIQUE (subdomain, lead_id, f_ult_msj_asesor)
        )
    ''')
    _agregar_columna(c, 'tiempos_respuesta', 'responsible_user_id', 'BIGINT')
    _agregar_columna(c, 'tiempos_respuesta', 'lead_nombre', 'TEXT')
    # Quien atendio segun el campo custom de la cuenta, cuando lo tiene. Es
    # distinto de responsible_user_id: ese es el DUEÑO del lead, y en las
    # cuentas que comparten usuario no identifica a nadie.
    _agregar_columna(c, 'tiempos_respuesta', 'asesor_nombre', 'TEXT')
    # Hora del PRIMER mensaje del cliente sin responder. 'F Ult msj cliente'
    # guarda el ULTIMO, asi que si el cliente escribe varias veces antes de que
    # le contesten, medir desde ahi subestima la espera (medido: 2.3x en la
    # mediana). NULL = no hay mensajes guardados de esa ventana; en ese caso se
    # cae al valor viejo.
    _agregar_columna(c, 'tiempos_respuesta', 'f_primer_msj_cliente', 'BIGINT')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_tiempos_sub_lead
                 ON tiempos_respuesta (subdomain, lead_id, f_ult_msj_asesor)''')
    # _fecha_minima hace MIN(capturado_at) por subdominio en CADA carga del
    # dashboard. Sin este indice es un scan de toda la tabla eventos, y con un
    # solo worker de gunicorn ese scan bloquea el webhook que este entrando.
    c.execute('''CREATE INDEX IF NOT EXISTS idx_eventos_sub_capturado
                 ON eventos (subdomain, capturado_at)''')
    # Mensajes entrantes del cliente, uno por fila. Kommo solo manda webhook de
    # los entrantes, y hasta ahora quedaban enterrados dentro del JSON de
    # 'eventos', imposibles de consultar sin parsear todo.
    c.execute('''
        CREATE TABLE IF NOT EXISTS mensajes_cliente (
            subdomain TEXT,
            lead_id TEXT,
            ts BIGINT,
            PRIMARY KEY (subdomain, lead_id, ts)
        )
    ''')
    # Como se llama quien escribio. En la mayoria de las cuentas el lead no
    # tiene nombre propio —entra desde WhatsApp o Instagram y nadie lo
    # renombra— y el dashboard mostraba "(sin nombre)": 100% de los leads en
    # autonica y 70% en gruporegalado. Kommo manda el nombre en cada mensaje
    # entrante (author][name], con author][type] = external, o sea el cliente),
    # asi que se guarda aca y se usa cuando el lead no trae el suyo.
    _agregar_columna(c, 'mensajes_cliente', 'cliente_nombre', 'TEXT')
    # La clave primaria es (subdomain, lead_id, ts), asi que 'ts' queda tercera
    # y una consulta por (subdomain, ts) no la puede usar. "Sin responder"
    # consulta exactamente asi —los mensajes desde el corte— y ahora ademas se
    # refresca sola cada minuto y medio, o sea muchas veces por sesion.
    c.execute('''CREATE INDEX IF NOT EXISTS idx_msjs_cliente_sub_ts
                 ON mensajes_cliente (subdomain, ts)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_msjs_asesor_sub_ts
                 ON mensajes_asesor (subdomain, ts)''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS leads_estado (
            subdomain TEXT,
            lead_id TEXT,
            responsible_user_id BIGINT,
            f_ult_msj_cliente BIGINT,
            f_ult_msj_asesor BIGINT,
            evento_ts BIGINT,
            actualizado_at TEXT,
            PRIMARY KEY (subdomain, lead_id)
        )
    ''')
    _agregar_columna(c, 'leads_estado', 'lead_nombre', 'TEXT')
    _agregar_columna(c, 'leads_estado', 'status_id', 'BIGINT')
    # El embudo. Llega en el 100% de los lead_update y hasta ahora se tiraba.
    # Sin esto no se puede filtrar por embudo, que es lo que se pidio en la
    # reunion: Camara China tiene 24 embudos y mezclarlos en un solo numero
    # hace que el dashboard promedie ventas con reclamos y con cursos.
    _agregar_columna(c, 'leads_estado', 'pipeline_id', 'BIGINT')
    # Tambien aca y no solo en tiempos_respuesta: las tarjetas de "Sin
    # responder" y "Atendidos hoy" leen esta tabla, y si mostraran el
    # responsable mientras el ranking muestra al asesor, el mismo lead
    # apareceria con dos nombres distintos en la misma pantalla.
    _agregar_columna(c, 'leads_estado', 'asesor_nombre', 'TEXT')

    # Mensajes SALIENTES: los que manda el asesor (o el bot) al cliente.
    #
    # Llegan bajo la clave 'outgoing_message[add][n]', no bajo 'message[add][n]'
    # como los entrantes. Por eso durante meses cayeron en tipo_evento
    # 'desconocido' y se concluyo que Kommo no los mandaba. Si los manda, pero
    # solo en las cuentas suscritas al evento add_outgoing_message.
    #
    # La clave es msg_id y no (lead_id, ts): el mismo asesor puede mandar dos
    # mensajes en el mismo segundo —pasa seguido, texto + foto— y ademas el
    # 28% de los salientes no trae lead_id, asi que no serviria como clave.
    #
    # NO se decide aca si el mensaje es de una persona o de un bot. Se guardan
    # user_id, author_type y autor_nombre crudos y la regla se aplica al leer,
    # porque la clasificacion todavia no esta resuelta: hay respuestas humanas
    # con user_id = 0 (el asesor contesto desde la app de WhatsApp y Kommo no
    # sabe quien fue) mezcladas con los saludos automaticos del Salesbot.
    c.execute('''
        CREATE TABLE IF NOT EXISTS mensajes_asesor (
            subdomain TEXT,
            msg_id TEXT,
            lead_id TEXT,
            ts BIGINT,
            user_id BIGINT,
            autor_nombre TEXT,
            author_type TEXT,
            chat_id TEXT,
            talk_id TEXT,
            origin TEXT,
            message_type TEXT,
            PRIMARY KEY (subdomain, msg_id)
        )
    ''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_msjs_asesor_lead
                 ON mensajes_asesor (subdomain, lead_id, ts)''')
    # Para el UPDATE que completa los lead_id que faltan.
    c.execute('''CREATE INDEX IF NOT EXISTS idx_msjs_asesor_chat
                 ON mensajes_asesor (subdomain, chat_id)
                 WHERE lead_id IS NULL''')

    # Puente chat -> lead. Los salientes traen chat_id siempre (100%) pero el
    # lead solo a veces (72% en tucoytico, 97% en Core Power). Los ENTRANTES
    # traen las dos cosas, asi que el mapa se arma con ellos y se usa para
    # completar los salientes huerfanos.
    #
    # La clave es chat_id y no talk_id: un mismo chat pasa por varios talk_id a
    # lo largo del tiempo (un lead tenia los talks 10838 y 16735 sobre un unico
    # chat), asi que talk_id no identifica la conversacion.
    c.execute('''
        CREATE TABLE IF NOT EXISTS conversaciones (
            subdomain TEXT,
            chat_id TEXT,
            lead_id TEXT,
            PRIMARY KEY (subdomain, chat_id)
        )
    ''')

    # Supabase expone una API REST publica ademas de la conexion Postgres. Esta
    # app no la usa —se conecta por DATABASE_URL— pero la API sigue abierta, y
    # sin RLS cualquiera con la URL del proyecto puede leer estas tablas. En
    # 'eventos' eso son telefonos, nombres y texto de los mensajes de los
    # clientes de los 6 clientes.
    #
    # Se activa RLS SIN crear politicas: la API REST queda sin acceso, y la app
    # no se ve afectada porque se conecta como dueño de las tablas y el dueño
    # salta RLS. Va en init_db para que un deploy futuro no lo deje sin activar.
    for tabla in ('eventos', 'tiempos_respuesta', 'mensajes_cliente', 'leads_estado',
                  'mensajes_asesor', 'conversaciones'):
        try:
            _activar_rls(c, tabla)
        except Exception as e:
            # No debe impedir el arranque: sin esto la app no procesa webhooks.
            print(f"⚠️  No se pudo activar RLS en {tabla}: {e}")
    conn.close()
    print("✅ Base de datos lista")

init_db()

# ========================
# HELPERS
# ========================
def get_custom_field(data, prefix, field_name):
    """Recorre los indices de custom_fields que realmente vienen en el payload.
    Un rango fijo (ej. range(20)) descarta en silencio los campos de leads con
    muchos custom fields: el lead deja de medirse sin ningun error visible."""
    for i in get_batch_indices(data, f'{prefix}[custom_fields]'):
        if data.get(f'{prefix}[custom_fields][{i}][name]') == field_name:
            val = data.get(f'{prefix}[custom_fields][{i}][values][0]')
            try:
                return int(val)
            except (TypeError, ValueError):
                return None
    return None

def get_custom_field_texto(data, prefix, field_name):
    """Igual que get_custom_field pero para campos de TEXTO o lista.

    Kommo aplana los dos tipos distinto y por eso hacen falta dos funciones:

        fecha:  [custom_fields][i][values][0]           -> 1786000000
        texto:  [custom_fields][i][values][0][value]    -> 'Ivis Anchundia'

    Leer un campo de texto con get_custom_field devuelve None sin ningun error
    —la clave plana no existe— asi que el campo parece vacio en vez de mal
    leido. Es exactamente lo que pasaba con 'Asesor' antes de mirarlo de cerca.
    """
    if not field_name:
        return None
    for i in get_batch_indices(data, f'{prefix}[custom_fields]'):
        if data.get(f'{prefix}[custom_fields][{i}][name]') == field_name:
            val = data.get(f'{prefix}[custom_fields][{i}][values][0][value]')
            val = (str(val).strip() if val is not None else '')[:120]
            return val or None
    return None

CAMPOS_DEFAULT = {'cliente': 'F Ult msj cliente', 'asesor': 'F ult msj asesor'}

def campo_quien_atendio_de(subdomain):
    """Nombre del campo custom que dice QUIEN atendio el lead.

    Ojo con no confundirlo con campos_de(subdomain)['asesor'], que es la FECHA
    del ultimo mensaje del asesor. Son dos campos distintos de Kommo y los dos
    tienen la palabra "asesor" en el nombre.

    Solo lo tienen las cuentas que comparten usuario de Kommo entre varias
    personas (hoy gruporegalado). Sin configurar, la cuenta mide por
    responsable exactamente como antes."""
    return PULSE_CONFIG.get(subdomain, {}).get('campo_quien_atendio')

def campos_de(subdomain):
    """Cada cuenta de Kommo nombra distinto sus campos custom de
    'ultimo mensaje cliente/asesor' (mayusculas, typos, sufijos).
    Se configuran por subdominio en PULSE_CONFIG['campos']."""
    return PULSE_CONFIG.get(subdomain, {}).get('campos', CAMPOS_DEFAULT)

def get_batch_indices(data, key_root):
    """Kommo aplana las listas en claves indexadas (message[add][0],
    message[add][1], ... / [custom_fields][0], [custom_fields][1], ...).
    Devuelve todos los índices realmente presentes bajo key_root."""
    idxs = set()
    pattern = re.compile(re.escape(key_root) + r'\[(\d+)\]')
    for k in data.keys():
        m = pattern.match(k)
        if m:
            idxs.add(int(m.group(1)))
    return sorted(idxs)

def prefix_de_lead(data, lead_id):
    """Ubica el prefijo leads[update][i] que corresponde a ESTE lead_id.

    Un mismo POST puede traer varios leads y en la tabla 'eventos' guardamos el
    payload completo en la fila de cada uno. Asumir el índice [0] al releer esas
    filas escribe las fechas del primer lead del batch bajo el lead_id de los
    demás. Devuelve None si el lead no está en el payload (mejor saltarlo que
    adivinar)."""
    for i in get_batch_indices(data, 'leads[update]'):
        if str(data.get(f'leads[update][{i}][id]')) == str(lead_id):
            return f'leads[update][{i}]'
    return None

def msjs_entrantes(data):
    """Extrae (lead_id, ts, nombre) de cada mensaje ENTRANTE del payload.

    Solo entrantes: Kommo no manda webhook de los mensajes del asesor (0 de
    ~5800 revisados en produccion tenian type=outgoing). Devuelve un set para
    que un mismo POST guardado varias veces no duplique.

    El nombre es el del autor del mensaje. Como solo entran los entrantes, el
    autor es siempre el cliente —se verifico author][type] = external en 150 de
    150 mensajes de tres cuentas— y es el unico nombre disponible para los
    leads que Kommo nunca bautizo."""
    salida = set()
    for i in get_batch_indices(data, 'message[add]'):
        pref = f'message[add][{i}]'
        if data.get(f'{pref}[type]') != 'incoming':
            continue
        lead_id = data.get(f'{pref}[element_id]')
        try:
            ts = int(data.get(f'{pref}[created_at]'))
        except (TypeError, ValueError):
            continue
        # El nombre puede faltar o venir vacio; el mensaje se guarda igual,
        # porque para medir la espera alcanza con lead_id y ts.
        nombre = (data.get(f'{pref}[author][name]') or '').strip()[:200] or None
        if lead_id and ts:
            salida.add((str(lead_id), ts, nombre))
    return salida

def insertar_msjs_cliente(cursor, subdomain, filas):
    """Inserta (lead_id, ts, nombre) en 'mensajes_cliente' en UNA sola ida.

    execute_values manda un INSERT multi-fila; executemany manda una sentencia
    por fila, y contra Supabase la latencia de red domina: en el backfill eso
    era la diferencia entre ~30 y varios miles de mensajes por segundo."""
    if not filas:
        return 0

    # Un mismo (lead_id, ts) no puede ir dos veces en el MISMO insert: con
    # ON CONFLICT DO UPDATE, Postgres aborta la sentencia entera con "cannot
    # affect row a second time". Pasa desapercibido hasta que ocurre, y
    # entonces se pierde la pagina completa del backfill.
    #
    # Puede darse porque 'filas' es un set de (lead_id, ts, nombre): el mismo
    # mensaje capturado dos veces con el contacto renombrado en el medio son
    # dos elementos distintos del set y una sola fila en la tabla. Se resuelve
    # antes de consultar, quedandose con la version que trae nombre.
    unicas = {}
    for lead_id, ts, nombre in filas:
        clave = (lead_id, ts)
        previo = unicas.get(clave)
        if previo is None or (previo[2] is None and nombre):
            unicas[clave] = (lead_id, ts, nombre)

    # DO UPDATE y no DO NOTHING: los mensajes viejos ya estan guardados sin
    # nombre, y con DO NOTHING el backfill no podria completarlos nunca. El
    # WHERE evita reescribir los que ya tienen uno, asi que reprocesar el
    # historial es idempotente.
    execute_values(
        cursor,
        'INSERT INTO mensajes_cliente (subdomain, lead_id, ts, cliente_nombre) VALUES %s '
        'ON CONFLICT (subdomain, lead_id, ts) DO UPDATE '
        'SET cliente_nombre = EXCLUDED.cliente_nombre '
        'WHERE mensajes_cliente.cliente_nombre IS NULL',
        [(subdomain, lead_id, ts, nombre) for lead_id, ts, nombre in unicas.values()]
    )
    return len(unicas)

def guardar_msjs_entrantes(subdomain, data):
    """Guarda los mensajes entrantes de UN payload (camino del webhook).

    Es la fuente para saber cuando empezo a esperar el cliente."""
    filas = msjs_entrantes(data)
    if not filas:
        return 0
    conn = get_conn()
    try:
        c = conn.cursor()
        n = insertar_msjs_cliente(c, subdomain, filas)
        conn.commit()
        _CARGA['escrituras_ok'] += 1
        return n
    except Exception as e:
        _fallo_escritura('mensajes_cliente', e)
        return 0
    finally:
        conn.close()

# ========================
# MENSAJES SALIENTES (del asesor)
# ========================
def msjs_salientes(data):
    """Extrae los mensajes SALIENTES del payload: los que se le mandan al cliente.

    Vienen bajo 'outgoing_message[add][n]'. Kommo los manda solo si la cuenta
    esta suscrita al evento add_outgoing_message, que no aparece en la pantalla
    de Kommo y hay que activar por API (scripts/suscribir_salientes.py).

    Se devuelve todo crudo, sin decidir si es del asesor o de un bot: esa regla
    no esta cerrada todavia. Lo unico seguro medido hasta ahora es que
    author][name] = 'Salesbot' es automatico, y que user_id = 0 NO alcanza para
    descartar, porque el asesor que contesta desde la app de WhatsApp llega con
    user_id 0 y nombre 'WhatsApp Business'.

    Clave por msg_id para que un mismo POST reprocesado no duplique."""
    salida = {}
    for i in get_batch_indices(data, 'outgoing_message[add]'):
        p = f'outgoing_message[add][{i}]'
        msg_id = str(data.get(f'{p}[id]') or '').strip()
        if not msg_id:
            continue
        try:
            ts = int(data.get(f'{p}[created_at]'))
        except (TypeError, ValueError):
            continue
        uid = str(data.get(f'{p}[author][user_id]') or '0').strip()
        salida[msg_id] = {
            'msg_id':       msg_id[:120],
            # element_id falta en ~1 de cada 4; se completa despues por chat_id.
            'lead_id':      str(data.get(f'{p}[element_id]') or '').strip() or None,
            'ts':           ts,
            'user_id':      int(uid) if uid.lstrip('-').isdigit() else 0,
            # Kommo escapa el nombre para HTML: llega "Ventas Tuco&amp;Tico".
            # Sin desescapar, ese &amp; termina impreso tal cual en el ranking.
            'autor_nombre': html.unescape(
                (data.get(f'{p}[author][name]') or '').strip())[:200] or None,
            'author_type':  (data.get(f'{p}[author][type]') or '').strip()[:40] or None,
            'chat_id':      str(data.get(f'{p}[chat_id]') or '').strip() or None,
            'talk_id':      str(data.get(f'{p}[talk_id]') or '').strip() or None,
            'origin':       (data.get(f'{p}[origin]') or '').strip()[:40] or None,
            'message_type': (data.get(f'{p}[message_type]') or '').strip()[:40] or None,
        }
    return list(salida.values())

def chats_de_entrantes(data):
    """(chat_id, lead_id) de cada mensaje ENTRANTE.

    Es el puente para los salientes que no traen lead. Sale de los entrantes
    porque son los unicos que traen las dos cosas juntas."""
    salida = {}
    for i in get_batch_indices(data, 'message[add]'):
        p = f'message[add][{i}]'
        chat = str(data.get(f'{p}[chat_id]') or '').strip()
        lead = str(data.get(f'{p}[element_id]') or '').strip()
        if chat and lead:
            salida[chat] = lead
    return salida

def insertar_conversaciones(cursor, subdomain, mapa):
    """Guarda el mapa chat -> lead. DO NOTHING y no DO UPDATE: si un chat ya
    esta asociado a un lead, reasignarlo movería mensajes viejos a otro lead."""
    if not mapa:
        return 0
    execute_values(
        cursor,
        'INSERT INTO conversaciones (subdomain, chat_id, lead_id) VALUES %s '
        'ON CONFLICT (subdomain, chat_id) DO NOTHING',
        [(subdomain, chat, lead) for chat, lead in mapa.items()]
    )
    return len(mapa)

def insertar_msjs_asesor(cursor, subdomain, filas):
    """Inserta los salientes en UNA sola ida, como los entrantes.

    ON CONFLICT DO UPDATE solo sobre lead_id: un mensaje puede haberse guardado
    sin lead y completarse despues. El resto de los campos no cambia nunca, asi
    que reprocesar el historial es idempotente."""
    if not filas:
        return 0
    # Un msg_id repetido dentro del MISMO insert aborta la sentencia entera con
    # "cannot affect row a second time" y se pierde la pagina completa.
    unicas = {}
    for f in filas:
        previo = unicas.get(f['msg_id'])
        if previo is None or (previo['lead_id'] is None and f['lead_id']):
            unicas[f['msg_id']] = f
    execute_values(
        cursor,
        'INSERT INTO mensajes_asesor (subdomain, msg_id, lead_id, ts, user_id, '
        'autor_nombre, author_type, chat_id, talk_id, origin, message_type) VALUES %s '
        'ON CONFLICT (subdomain, msg_id) DO UPDATE '
        'SET lead_id = COALESCE(mensajes_asesor.lead_id, EXCLUDED.lead_id), '
        # Tambien el nombre: sale del mismo payload y es deterministico, asi que
        # refrescarlo permite corregir hacia atras (por ejemplo el &amp;) con
        # solo volver a correr el backfill.
        '    autor_nombre = EXCLUDED.autor_nombre',
        [(subdomain, f['msg_id'], f['lead_id'], f['ts'], f['user_id'], f['autor_nombre'],
          f['author_type'], f['chat_id'], f['talk_id'], f['origin'], f['message_type'])
         for f in unicas.values()]
    )
    return len(unicas)

# Completa los salientes que quedaron sin lead usando el mapa chat -> lead.
# Hace falta un paso aparte porque el entrante que revela el lead puede llegar
# DESPUES del saliente (o no haber llegado todavia cuando se guardo).
SQL_COMPLETAR_LEAD_SALIENTE = '''
    UPDATE mensajes_asesor m
       SET lead_id = c.lead_id
      FROM conversaciones c
     WHERE m.subdomain = c.subdomain
       AND m.chat_id   = c.chat_id
       AND m.lead_id IS NULL
       AND m.subdomain = %s
'''

def guardar_msjs_salientes(subdomain, data):
    """Camino del webhook: guarda los salientes de UN payload."""
    filas = msjs_salientes(data)
    if not filas:
        return 0
    conn = get_conn()
    try:
        c = conn.cursor()
        n = insertar_msjs_asesor(c, subdomain, filas)
        # Los que llegaron sin lead se completan enseguida si el chat ya se
        # conoce. Es una consulta acotada a los chats de ESTE payload, no un
        # UPDATE sobre toda la tabla.
        chats = [f['chat_id'] for f in filas if not f['lead_id'] and f['chat_id']]
        if chats:
            c.execute(
                'UPDATE mensajes_asesor m SET lead_id = c.lead_id '
                'FROM conversaciones c '
                'WHERE m.subdomain = c.subdomain AND m.chat_id = c.chat_id '
                'AND m.lead_id IS NULL AND m.subdomain = %s AND m.chat_id = ANY(%s)',
                (subdomain, chats)
            )
        conn.commit()
        _CARGA['escrituras_ok'] += 1
        return n
    except Exception as e:
        conn.rollback()
        _fallo_escritura('mensajes_asesor', e)
        return 0
    finally:
        conn.close()

def guardar_conversaciones(subdomain, data):
    """Camino del webhook: guarda el mapa chat -> lead de los entrantes."""
    mapa = chats_de_entrantes(data)
    if not mapa:
        return 0
    conn = get_conn()
    try:
        c = conn.cursor()
        n = insertar_conversaciones(c, subdomain, mapa)
        conn.commit()
        _CARGA['escrituras_ok'] += 1
        return n
    except Exception as e:
        conn.rollback()
        _fallo_escritura('conversaciones', e)
        return 0
    finally:
        conn.close()

# ========================
# GUARDAR EVENTO
# ========================
# Los unicos tipos cuyo JSON crudo se vuelve a LEER: los backfills lo releen
# para reconstruir hacia atras (/backfill, /backfill-mensajes,
# /backfill-salientes, /backfill-embudo). Del resto solo importa que llego.
TIPOS_CON_CRUDO = {'mensaje', 'mensaje_saliente', 'lead_update'}

# Cuantos ejemplos completos se guardan de cada FORMA de evento desconocido.
# Con uno alcanzaria para ver que trae; van 3 por si el primero llega vacio o
# recortado. Pasados esos, el evento no se guarda: ya sabemos que existe y que
# no lo usamos, y la fila numero 40.000 no agrega nada.
EJEMPLOS_POR_FORMA = 3

# Techo de formas distintas a recordar. Sin techo, un payload raro que cambie
# de claves seguido haria crecer este diccionario sin limite dentro del
# proceso. 500 es holgado: hoy Kommo manda una docena de formas.
TOPE_FORMAS = 500

# Formas de payload ya vistas EN ESTE PROCESO, con cuantas guardamos de cada
# una. Se reinicia con cada deploy a proposito: lo que se busca es poder mirar
# un evento nuevo, no acumular copias del mismo.
_FORMAS_VISTAS = Counter()

def _que_guardar(tipo_evento, data):
    """Decide si el evento se guarda, y con cuanto detalle.

    Devuelve (guardar, raw_data). guardar=False significa que el evento no
    deja fila: se cuenta en /health/carga y nada mas.

    Esto es lo que lleno el disco. Estabamos suscritos a 60 eventos de Kommo y
    leemos 5: los otros 55 —talk, contacts, notes, tasks, catalogos— caian en
    tipo_evento='desconocido' y se guardaban ENTEROS. 278.147 filas que nadie
    releyo nunca, y contacts[update] trae todos los campos personalizados del
    contacto. El 31/08/2026 la base llego al limite, Supabase la puso en modo
    solo lectura y estuvimos 43 horas recibiendo sin guardar nada.

    La regla queda al reves que antes:

      mensaje / mensaje_saliente / lead_update
          fila + JSON entero. Son los unicos que los backfills releen.
      lead_status / lead_responsible
          fila sin JSON. Queda el registro de que el evento llego, que es lo
          unico que se mira, y se va lo que pesa.
      desconocido
          los primeros EJEMPLOS_POR_FORMA de cada forma, enteros. El resto no
          se guarda.

    Guardar esos primeros ejemplos no es un lujo: asi descubrimos que Kommo SI
    manda los mensajes salientes, bajo una clave que no estabamos mirando. Lo
    que no hacia falta era la copia numero mil."""
    if tipo_evento in TIPOS_CON_CRUDO:
        return True, json.dumps(data)

    if tipo_evento != 'desconocido':
        return True, None

    patrones = sorted({re.sub(r'\[\d+\]', '[n]', k) for k in data})
    firma = '|'.join(patrones)
    vistas = _FORMAS_VISTAS.get(firma, 0)
    if vistas >= EJEMPLOS_POR_FORMA:
        return False, None
    if vistas == 0 and len(_FORMAS_VISTAS) >= TOPE_FORMAS:
        return False, None
    _FORMAS_VISTAS[firma] = vistas + 1
    return True, json.dumps(data)

def guardar_evento(subdomain, tipo_evento, lead_id, timestamp, data):
    guardar, crudo = _que_guardar(tipo_evento, data)
    if not guardar:
        # Se cuenta, no se descarta en silencio: si algun dia hay que volver a
        # mirar un evento que hoy ignoramos, este numero dice cuanto nos
        # estamos perdiendo y /health/desconocidos tiene los ejemplos.
        _CARGA['eventos_no_guardados'] += 1
        return
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO eventos (subdomain, tipo_evento, lead_id, timestamp, raw_data, capturado_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            subdomain,
            tipo_evento,
            lead_id,
            int(timestamp) if timestamp else None,
            crudo,
            datetime.now().isoformat()
        ))
        conn.commit()
        _CARGA['escrituras_ok'] += 1
        print(f"💾 {subdomain} | {tipo_evento} | lead: {lead_id}")
    except Exception as e:
        _fallo_escritura('eventos', e)
    finally:
        conn.close()

def nombre_asesor(responsible_user_id):
    if responsible_user_id in (None, '', 0, '0'):
        return None
    return ASESORES.get(int(responsible_user_id), f'Usuario {responsible_user_id}')

def nombre_de_remitente(user_id, nombre_del_mensaje):
    """Como se llama el que mando un mensaje saliente.

    Manda NUESTRA lista sobre el nombre que viene en el mensaje, cuando el
    user_id se conoce. Los dos son la misma persona escrita distinto —Kommo
    manda "Luis Marroquin" y nuestra lista dice "Luis Marroquín"— y esa tilde
    partia al mismo asesor en dos filas del ranking: 310 respuestas bajo una
    grafia y 22 bajo la otra, cada una con su mediana y ninguna real.

    Se elige nuestra lista y no la de Kommo porque es la unica que tambien
    cubre lo anterior al corte, donde solo hay responsible_user_id. Si mandara
    el nombre del mensaje, el mismo asesor volveria a partirse en dos: una
    grafia antes del corte y otra despues.

    Con user_id 0 —bots e integraciones como Wazzup— no hay a quien resolver y
    queda el nombre del mensaje, que es el del canal."""
    if user_id:
        propio = nombre_asesor(user_id)
        # nombre_asesor devuelve 'Usuario 123' cuando no lo conoce. Ahi el
        # nombre del mensaje dice mas que un numero.
        if propio and not propio.startswith('Usuario '):
            return propio
    return nombre_del_mensaje

def remitentes_auto_de(subdomain):
    """Los remitentes que no son personas en esta cuenta."""
    return set(PULSE_CONFIG.get(subdomain, {}).get(
        'remitentes_automaticos', REMITENTES_AUTOMATICOS))

def asesor_de_registro(r):
    """Quien atendio una respuesta.

    Cuatro fuentes, en este orden:

      1. Si el mensaje lo mando un BOT, gana el nombre del bot. Va primero
         porque el campo custom es del LEAD, no del mensaje: si el Salesbot
         saluda en un lead que atiende Ivis, atribuirle a Ivis esa respuesta de
         4 segundos le mejora la mediana con algo que no hizo.
      2. El campo custom 'Asesor' del lead. Solo lo carga Camara China, y ahi
         es mejor que el remitente del mensaje: todos comparten el usuario
         'Ventas', asi que el mensaje dice 'Ventas' y el campo dice la persona.
      3. Quien mando el mensaje, PERO solo si Kommo lo identifico.
      4. El responsable del lead, que es lo unico que habia antes del corte.

    Y si no hay ninguna de las cuatro, devuelve None y el ranking lo agrupa
    bajo 'Sin asignar'.

    Sobre el paso 3: user_id 0 significa que la respuesta salio por un canal
    compartido —WhatsApp Lite, Wazzup, la app de WhatsApp Business— y entonces
    el nombre que trae el mensaje es el del CANAL, no el de quien apreto
    enviar. Medido sobre las seis cuentas: 5.994 de 17.820 salientes, el 34%,
    llegan asi. Poner "WhatsApp Lite" en el ranking no dice nada de nadie; el
    responsable del lead, al menos, es una persona.

    Donde el canal SI identifica al usuario —Ventas Directas, o cualquier
    respuesta desde la web— el paso 3 sigue ganandole al responsable, que es
    lo correcto: ahi sabemos quien contesto de verdad y el responsable seria
    una aproximacion peor.

    La regla vive en un solo lugar porque la usan el ranking, la tendencia
    diaria y el detalle de la distribucion; si se separan, el dashboard puede
    mostrar un nombre en el ranking y otro distinto al abrir la franja."""
    if r.get('respondio_auto'):
        return r.get('respondio_nombre')
    if r.get('asesor_nombre'):
        return r['asesor_nombre']
    if r.get('respondio_user_id'):
        return r.get('respondio_nombre')
    return nombre_asesor(r.get('responsible_user_id'))

def guardar_tiempo_respuesta(subdomain, lead_id, f_cliente, f_asesor, responsible_user_id=None,
                             lead_nombre=None, asesor_nombre=None):
    tiempo_seg = f_asesor - f_cliente
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO tiempos_respuesta
                (subdomain, lead_id, f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, capturado_at, responsible_user_id, lead_nombre, asesor_nombre)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (subdomain, lead_id, f_ult_msj_asesor)
            DO UPDATE SET responsible_user_id = EXCLUDED.responsible_user_id,
                          lead_nombre   = EXCLUDED.lead_nombre,
                          -- COALESCE y no pisar a secas: si el lead se toca
                          -- despues sin el campo cargado, se perderia quien
                          -- habia atendido.
                          asesor_nombre = COALESCE(EXCLUDED.asesor_nombre, tiempos_respuesta.asesor_nombre)
        ''', (subdomain, lead_id, f_cliente, f_asesor, tiempo_seg, datetime.now().isoformat(), responsible_user_id, lead_nombre, asesor_nombre))
        conn.commit()
        _CARGA['escrituras_ok'] += 1
        print(f"⏱️  {subdomain} | lead {lead_id} | asesor respondió en {tiempo_seg // 60}min {tiempo_seg % 60}s")
    except Exception as e:
        _fallo_escritura('tiempos_respuesta', e)
    finally:
        conn.close()

def guardar_lead_estado(subdomain, lead_id, responsible_user_id, f_cliente, f_asesor, evento_ts,
                        lead_nombre=None, status_id=None, asesor_nombre=None,
                        pipeline_id=None):
    """Guarda el estado vigente del lead (quién es responsable, cuándo escribió
    cliente/asesor por última vez, en qué status esta). Se llama en CADA
    leads[update], sin importar quién respondió último. evento_ts evita que
    un evento viejo (ej. reprocesado por /backfill) pise un estado más reciente.

    status_id 142/143 son constantes reservadas de Kommo (Ganado/Perdido),
    iguales en todas las cuentas - se usan para filtrar "solo abiertas"."""
    if not evento_ts:
        return
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO leads_estado
                (subdomain, lead_id, responsible_user_id, f_ult_msj_cliente, f_ult_msj_asesor, evento_ts, actualizado_at, lead_nombre, status_id, asesor_nombre, pipeline_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (subdomain, lead_id) DO UPDATE SET
                responsible_user_id = COALESCE(EXCLUDED.responsible_user_id, leads_estado.responsible_user_id),
                f_ult_msj_cliente   = COALESCE(EXCLUDED.f_ult_msj_cliente,   leads_estado.f_ult_msj_cliente),
                f_ult_msj_asesor    = COALESCE(EXCLUDED.f_ult_msj_asesor,    leads_estado.f_ult_msj_asesor),
                evento_ts           = EXCLUDED.evento_ts,
                actualizado_at      = EXCLUDED.actualizado_at,
                lead_nombre         = COALESCE(EXCLUDED.lead_nombre,         leads_estado.lead_nombre),
                status_id           = COALESCE(EXCLUDED.status_id,           leads_estado.status_id),
                asesor_nombre       = COALESCE(EXCLUDED.asesor_nombre,       leads_estado.asesor_nombre),
                pipeline_id         = COALESCE(EXCLUDED.pipeline_id,         leads_estado.pipeline_id)
            WHERE EXCLUDED.evento_ts >= leads_estado.evento_ts
        ''', (subdomain, lead_id, responsible_user_id, f_cliente, f_asesor, int(evento_ts), datetime.now().isoformat(), lead_nombre, status_id, asesor_nombre, pipeline_id))
        conn.commit()
        _CARGA['escrituras_ok'] += 1
    except Exception as e:
        _fallo_escritura('leads_estado', e)
    finally:
        conn.close()

# ========================
# WEBHOOK
# ========================
@app.route('/webhook', methods=['POST'])
def webhook():
    # Kommo desactiva el webhook si la respuesta tarda o falla, y el mensaje que
    # muestra al hacerlo es siempre el mismo: "Webhook esta desactivado debido a
    # una respuesta no valida". O sea que lo que lo dispara no es la lentitud
    # sino el codigo de respuesta: cualquier cosa que no sea 200 lo apaga.
    #
    # Por eso este endpoint NUNCA puede propagar una excepcion. Se responde 200
    # pase lo que pase y el guardado corre aparte; perder un evento suelto es
    # mucho mas barato que perder la cuenta entera por horas o dias.
    try:
        data = request.form.to_dict()
    except Exception as e:
        _CARGA['errores_body'] += 1
        print(f"❌ No se pudo leer el body del webhook: {e}")
        return jsonify({'status': 'ok'}), 200

    try:
        threading.Thread(target=_procesar_webhook, args=(data,), daemon=True).start()
    except RuntimeError as e:
        # No se pudo crear el hilo: el proceso se quedo sin memoria o llego al
        # limite de hilos. Es el candidato mas directo a las desactivaciones,
        # porque hasta ahora salia como 500 —justo lo que Kommo no tolera— y
        # ademas ocurre precisamente en las rafagas, que es cuando se cayo.
        _CARGA['hilos_rechazados'] += 1
        print(f"❌ Sin capacidad para crear el hilo, evento descartado: {e}")

    # Medicion de carga. Kommo desactivo el webhook cuatro veces en tres semanas
    # (tucoytico, gruporegalado, ventasdirectas x2) y venimos suponiendo la
    # causa sin medirla: que las rafagas crean cientos de hilos, se agotan las
    # conexiones a Supabase y las respuestas se vuelven lentas.
    #
    # Como cada webhook lanza un hilo sin limite, el pico de hilos es la prueba
    # directa. Si en una rafaga sube a cientos, la hipotesis se confirma; si se
    # mantiene bajo, hay que buscar la causa en otro lado.
    #
    # No se usa lock: son contadores de diagnostico y el costo de perder alguna
    # suma bajo carga es menor que el de agregar contencion al camino critico.
    hilos = threading.active_count()
    _CARGA['webhooks'] += 1
    if hilos > _CARGA['hilos_maximo']:
        _CARGA['hilos_maximo'] = hilos
        _CARGA['cuando_el_maximo'] = datetime.utcnow().isoformat()
    return jsonify({'status': 'ok'}), 200

def _procesar_webhook(data):
    try:
        subdomain = data.get('account[subdomain]', 'desconocido')
        algo_procesado = False

        idxs_msj = get_batch_indices(data, 'message[add]')
        for i in idxs_msj:
            algo_procesado = True
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='mensaje',
                lead_id=data.get(f'message[add][{i}][element_id]'),
                timestamp=data.get(f'message[add][{i}][created_at]'),
                data=data
            )
        if idxs_msj:
            # Ademas del evento crudo, cada mensaje entrante va a su propia
            # fila para poder saber desde cuando espera el cliente.
            guardar_msjs_entrantes(subdomain, data)
            # Los entrantes son los unicos que traen chat_id y lead juntos: es
            # el puente para pegarle un lead a los salientes que no lo traen.
            guardar_conversaciones(subdomain, data)

        # Mensajes SALIENTES. Hasta ahora caian en 'desconocido' porque la clave
        # es 'outgoing_message[add]' y aca solo se buscaba 'message[add]'.
        idxs_sal = get_batch_indices(data, 'outgoing_message[add]')
        for i in idxs_sal:
            algo_procesado = True
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='mensaje_saliente',
                lead_id=data.get(f'outgoing_message[add][{i}][element_id]'),
                timestamp=data.get(f'outgoing_message[add][{i}][created_at]'),
                data=data
            )
        if idxs_sal:
            guardar_msjs_salientes(subdomain, data)

        for i in get_batch_indices(data, 'leads[update]'):
            algo_procesado = True
            lead_id = data.get(f'leads[update][{i}][id]')
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_update',
                lead_id=lead_id,
                timestamp=data.get(f'leads[update][{i}][updated_at]'),
                data=data
            )
            # Calcular tiempo de respuesta del asesor
            prefix = f'leads[update][{i}]'
            campos = campos_de(subdomain)
            f_cliente = get_custom_field(data, prefix, campos['cliente'])
            f_asesor = get_custom_field(data, prefix, campos['asesor'])
            responsible_user_id = data.get(f'{prefix}[responsible_user_id]')
            evento_ts = data.get(f'{prefix}[updated_at]')
            lead_nombre = data.get(f'{prefix}[name]')
            status_id = data.get(f'{prefix}[status_id]')
            pipeline_id = data.get(f'{prefix}[pipeline_id]')
            # Solo en las cuentas que lo configuraron; en el resto queda None y
            # todo sigue midiendose por responsable como hasta ahora.
            asesor_nombre = get_custom_field_texto(data, prefix, campo_quien_atendio_de(subdomain))

            # Estado vigente del lead (para "no respondidos" / "trabajados hoy"),
            # se guarda siempre, sin importar quién escribió último.
            guardar_lead_estado(subdomain, lead_id, responsible_user_id, f_cliente, f_asesor,
                                evento_ts, lead_nombre, status_id, asesor_nombre, pipeline_id)

            if f_cliente and f_asesor and f_asesor > f_cliente:
                guardar_tiempo_respuesta(subdomain, lead_id, f_cliente, f_asesor,
                                         responsible_user_id, lead_nombre, asesor_nombre)

        for i in get_batch_indices(data, 'leads[status]'):
            algo_procesado = True
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_status',
                lead_id=data.get(f'leads[status][{i}][id]'),
                timestamp=data.get(f'leads[status][{i}][updated_at]'),
                data=data
            )

        for i in get_batch_indices(data, 'leads[responsible]'):
            algo_procesado = True
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_responsible',
                lead_id=data.get(f'leads[responsible][{i}][id]'),
                timestamp=data.get(f'leads[responsible][{i}][updated_at]'),
                data=data
            )

        if not algo_procesado:
            # Se GUARDA, no solo se loguea. Antes se imprimian las dos primeras
            # claves y se descartaba el resto, asi que no habia forma de saber
            # que traian estos POST — y llegan a cientos por rafaga.
            #
            # Importa porque podrian ser los mensajes SALIENTES del asesor: el
            # dato que falta para medir bien el tiempo de respuesta y saber
            # quien contesto. Dimos por hecho que Kommo no los manda, pero eso
            # se concluyo mirando los eventos que si reconocemos. Si vienen bajo
            # una clave distinta, los estabamos tirando sin verlos.
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='desconocido',
                lead_id=None,
                timestamp=None,
                data=data
            )
            # Patrones de clave (indices reemplazados por [n]) en vez de las dos
            # primeras: con dos claves no se distingue un tipo de evento de otro.
            patrones = sorted({re.sub(r'\[\d+\]', '[n]', k) for k in data})[:15]
            print(f"⚠️  Ignorado | {subdomain} | {patrones}")

    except Exception as e:
        print(f"❌ ERROR procesando webhook en background: {e}")

# ========================
# ENDPOINTS
# ========================
@app.route('/')
@token_requerido
def home():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute('SELECT subdomain, tipo_evento, COUNT(*) FROM eventos GROUP BY subdomain, tipo_evento')
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM eventos')
        total = c.fetchone()[0]
        resumen = {}
        for subdomain, tipo, count in rows:
            if subdomain not in resumen:
                resumen[subdomain] = {}
            resumen[subdomain][tipo] = count
        return jsonify({'total': total, 'por_cliente': resumen})
    finally:
        conn.close()

@app.route('/data')
@token_requerido
def ver_datos():
    subdomain = request.args.get('subdomain')
    tipo = request.args.get('tipo')
    lead = request.args.get('lead_id')

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        query = 'SELECT * FROM eventos WHERE 1=1'
        params = []
        if subdomain:
            query += ' AND subdomain = %s'
            params.append(subdomain)
        if tipo:
            query += ' AND tipo_evento = %s'
            params.append(tipo)
        if lead:
            query += ' AND lead_id = %s'
            params.append(lead)
        query += ' ORDER BY timestamp DESC LIMIT 50'
        c.execute(query, params)
        rows = [dict(r) for r in c.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()

@app.route('/respuestas')
@token_requerido
def ver_respuestas():
    subdomain = request.args.get('subdomain')
    lead = request.args.get('lead_id')

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)

        # Resumen agregado por subdomain
        c.execute('''
            SELECT
                subdomain,
                COUNT(*) AS total_mediciones,
                ROUND(AVG(tiempo_respuesta_seg))       AS promedio_seg,
                ROUND(AVG(tiempo_respuesta_seg) / 60.0, 1) AS promedio_min,
                MIN(tiempo_respuesta_seg)              AS minimo_seg,
                MAX(tiempo_respuesta_seg)              AS maximo_seg
            FROM tiempos_respuesta
            GROUP BY subdomain
        ''')
        resumen = [dict(r) for r in c.fetchall()]

        # Detalle con filtros opcionales
        query = 'SELECT * FROM tiempos_respuesta WHERE 1=1'
        params = []
        if subdomain:
            query += ' AND subdomain = %s'
            params.append(subdomain)
        if lead:
            query += ' AND lead_id = %s'
            params.append(lead)
        query += ' ORDER BY f_ult_msj_asesor DESC LIMIT 100'
        c.execute(query, params)
        detalle = [dict(r) for r in c.fetchall()]
        for r in detalle:
            r['asesor'] = asesor_de_registro(r)

        return jsonify({'resumen': resumen, 'detalle': detalle})
    finally:
        conn.close()

@app.route('/backfill')
@token_requerido
def backfill():
    offset    = int(request.args.get('offset', 0))
    subdomain = request.args.get('subdomain')
    limit     = 300

    conn = get_conn()
    try:
        read_c  = conn.cursor(cursor_factory=RealDictCursor)
        write_c = conn.cursor()

        if subdomain:
            read_c.execute(
                "SELECT subdomain, lead_id, raw_data FROM eventos WHERE tipo_evento = 'lead_update' AND subdomain = %s ORDER BY id DESC LIMIT %s OFFSET %s",
                (subdomain, limit, offset)
            )
        else:
            read_c.execute(
                "SELECT subdomain, lead_id, raw_data FROM eventos WHERE tipo_evento = 'lead_update' ORDER BY id DESC LIMIT %s OFFSET %s",
                (limit, offset)
            )
        rows = read_c.fetchall()

        procesados = 0
        insertados = 0
        errores    = 0

        for row in rows:
            procesados += 1
            try:
                if not row['raw_data']:
                    continue
                data      = json.loads(row['raw_data'])
                prefix    = prefix_de_lead(data, row['lead_id'])
                if prefix is None:
                    errores += 1
                    print(f"⚠️ Backfill: lead {row['lead_id']} no está en su propio raw_data")
                    continue
                campos    = campos_de(row['subdomain'])
                f_cliente = get_custom_field(data, prefix, campos['cliente'])
                f_asesor  = get_custom_field(data, prefix, campos['asesor'])
                responsible_user_id = data.get(f'{prefix}[responsible_user_id]')
                evento_ts = data.get(f'{prefix}[updated_at]')
                lead_nombre = data.get(f'{prefix}[name]')
                status_id = data.get(f'{prefix}[status_id]')
                pipeline_id = data.get(f'{prefix}[pipeline_id]')
                asesor_nombre = get_custom_field_texto(data, prefix, campo_quien_atendio_de(row['subdomain']))

                if evento_ts:
                    write_c.execute('''
                        INSERT INTO leads_estado
                            (subdomain, lead_id, responsible_user_id, f_ult_msj_cliente, f_ult_msj_asesor, evento_ts, actualizado_at, lead_nombre, status_id, asesor_nombre, pipeline_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (subdomain, lead_id) DO UPDATE SET
                            responsible_user_id = COALESCE(EXCLUDED.responsible_user_id, leads_estado.responsible_user_id),
                            f_ult_msj_cliente   = COALESCE(EXCLUDED.f_ult_msj_cliente,   leads_estado.f_ult_msj_cliente),
                            f_ult_msj_asesor    = COALESCE(EXCLUDED.f_ult_msj_asesor,    leads_estado.f_ult_msj_asesor),
                            evento_ts           = EXCLUDED.evento_ts,
                            actualizado_at      = EXCLUDED.actualizado_at,
                            lead_nombre         = COALESCE(EXCLUDED.lead_nombre,         leads_estado.lead_nombre),
                            status_id           = COALESCE(EXCLUDED.status_id,           leads_estado.status_id),
                            asesor_nombre       = COALESCE(EXCLUDED.asesor_nombre,       leads_estado.asesor_nombre),
                            pipeline_id         = COALESCE(EXCLUDED.pipeline_id,         leads_estado.pipeline_id)
                        WHERE EXCLUDED.evento_ts >= leads_estado.evento_ts
                    ''', (
                        row['subdomain'], row['lead_id'], responsible_user_id,
                        f_cliente, f_asesor, int(evento_ts), datetime.now().isoformat(), lead_nombre, status_id, asesor_nombre, pipeline_id
                    ))

                if f_cliente and f_asesor and f_asesor > f_cliente:
                    write_c.execute('''
                        INSERT INTO tiempos_respuesta
                            (subdomain, lead_id, f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, capturado_at, responsible_user_id, lead_nombre, asesor_nombre)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (subdomain, lead_id, f_ult_msj_asesor)
                        DO UPDATE SET responsible_user_id = EXCLUDED.responsible_user_id,
                                      lead_nombre   = EXCLUDED.lead_nombre,
                                      asesor_nombre = COALESCE(EXCLUDED.asesor_nombre, tiempos_respuesta.asesor_nombre)
                    ''', (
                        row['subdomain'], row['lead_id'],
                        f_cliente, f_asesor, f_asesor - f_cliente,
                        datetime.now().isoformat(), responsible_user_id, lead_nombre, asesor_nombre
                    ))
                    if write_c.rowcount > 0:
                        insertados += 1
            except Exception as e:
                errores += 1
                print(f"⚠️ Backfill skip lead {row.get('lead_id')}: {e}")
                continue

        conn.commit()
        hay_mas = len(rows) == limit
        return jsonify({
            'offset': offset,
            'procesados': procesados,
            'insertados': insertados,
            'errores': errores,
            # El subdomain TIENE que viajar en 'siguiente'. Sin el, quien siga
            # la paginacion arranca filtrando una cuenta y desde la segunda
            # pagina pasa a recorrer las seis, con los offsets contra una lista
            # distinta: no se rompe nada —el backfill es idempotente— pero
            # nunca termina de cubrir la cuenta que se pidio y no hay forma de
            # notarlo desde afuera. /backfill-mensajes ya lo hacia bien.
            'siguiente': (f'/backfill?offset={offset + limit}'
                          + (f'&subdomain={subdomain}' if subdomain else '')) if hay_mas else None,
            'subdomain': subdomain,
            'listo': not hay_mas
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# SQL que recalcula, para cada respuesta del asesor, cuando empezo REALMENTE a
# esperar el cliente: el primer mensaje suyo posterior a la respuesta anterior
# del asesor en ese lead. Es re-ejecutable (idempotente) y solo toca filas que
# tengan mensajes guardados; el resto queda en NULL y cae al valor viejo.
#
# La ventana se acota contra f_ult_msj_asesor, NO contra f_ult_msj_cliente:
# f_ult_msj_cliente lo escribe una automatizacion de Kommo y no coincide al
# segundo con el created_at real del mensaje. Acotar por ahi descartaba
# mensajes validos por unos segundos de deriva entre los dos relojes
# (cobertura medida: 11% con esa condicion contra ~29% sin ella).
SQL_RECALCULAR_PRIMER_MSJ = '''
    UPDATE tiempos_respuesta t
    SET f_primer_msj_cliente = (
        SELECT MIN(m.ts)
        FROM mensajes_cliente m
        WHERE m.subdomain = t.subdomain
          AND m.lead_id   = t.lead_id
          AND m.ts        < t.f_ult_msj_asesor
          AND m.ts        > t.f_ult_msj_asesor - %s
          AND m.ts        > COALESCE((
                SELECT MAX(t2.f_ult_msj_asesor)
                FROM tiempos_respuesta t2
                WHERE t2.subdomain        = t.subdomain
                  AND t2.lead_id          = t.lead_id
                  AND t2.f_ult_msj_asesor < t.f_ult_msj_asesor
              ), 0)
    )
    WHERE t.subdomain = %s
      AND t.f_ult_msj_cliente IS NOT NULL
'''

@app.route('/backfill-mensajes')
@token_requerido
def backfill_mensajes():
    """Puebla 'mensajes_cliente' con los mensajes entrantes que ya estan
    guardados dentro del JSON de 'eventos', y recalcula f_primer_msj_cliente.

    Esto es lo que hace RETROACTIVO el arreglo de la metrica: sin esto, solo
    los mensajes que lleguen de ahora en adelante tendrian la espera bien
    medida. Paginado igual que /backfill.

    Cada llamada procesa lo que alcance en PRESUPUESTO_SEG y devuelve en
    'siguiente' donde continuar. Sin ese limite haria falta una llamada por
    cada 500 eventos, y hay decenas de miles.

    Con ?recalcular=1 (o al terminar la ultima pagina) corre el UPDATE que
    recalcula el inicio de la espera. Ese paso es idempotente."""
    subdomain = request.args.get('subdomain', '').strip()
    if subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain invalido', 'validos': list(PULSE_CONFIG)}), 400
    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0
    limit = 500
    # Por debajo del timeout tipico de un cliente HTTP (60s), para que la
    # respuesta llegue en vez de cortarse a mitad.
    PRESUPUESTO_SEG = 45

    conn = get_conn()
    try:
        read_c  = conn.cursor(cursor_factory=RealDictCursor)
        write_c = conn.cursor()

        arranque  = time.monotonic()
        guardados = 0
        errores   = 0
        leidos    = 0
        chats_vistos = 0
        hay_mas   = True
        while hay_mas and time.monotonic() - arranque < PRESUPUESTO_SEG:
            read_c.execute(
                "SELECT raw_data FROM eventos "
                "WHERE subdomain = %s AND tipo_evento = 'mensaje' "
                "ORDER BY id DESC LIMIT %s OFFSET %s",
                (subdomain, limit, offset)
            )
            rows = read_c.fetchall()
            # Acumular toda la pagina y hacer UN solo insert: un round-trip por
            # evento contra Supabase daba ~30 mensajes/seg, inviable para las
            # decenas de miles de eventos que hay.
            filas = set()
            chats = {}
            for row in rows:
                if not row['raw_data']:
                    continue
                try:
                    payload = json.loads(row['raw_data'])
                    filas |= msjs_entrantes(payload)
                    # Mismo parseo, dos usos: de aca sale el puente chat -> lead
                    # que necesitan los salientes que no traen el lead adentro.
                    chats.update(chats_de_entrantes(payload))
                except Exception as e:
                    errores += 1
                    print(f"⚠️ Backfill mensajes: {e}")
            guardados += insertar_msjs_cliente(write_c, subdomain, filas)
            chats_vistos += insertar_conversaciones(write_c, subdomain, chats)
            leidos  += len(rows)
            offset  += len(rows)
            hay_mas  = len(rows) == limit

        recalculado = None
        if not hay_mas or request.args.get('recalcular') == '1':
            write_c.execute(SQL_RECALCULAR_PRIMER_MSJ, (GAP_REACTIVACION_SEG, subdomain))
            recalculado = write_c.rowcount
        conn.commit()

        read_c.execute(
            'SELECT COUNT(*) AS total FROM mensajes_cliente WHERE subdomain = %s',
            (subdomain,)
        )
        total_msjs = read_c.fetchone()['total']
        read_c.execute(
            '''SELECT COUNT(*) AS total,
                      COUNT(f_primer_msj_cliente) AS con_inicio_real
               FROM tiempos_respuesta WHERE subdomain = %s''',
            (subdomain,)
        )
        cob = read_c.fetchone()

        return jsonify({
            'offset':                  offset,
            'eventos_procesados':      leidos,
            'segundos':                round(time.monotonic() - arranque, 1),
            'mensajes_encontrados':    guardados,
            'chats_mapeados':          chats_vistos,
            'errores':                 errores,
            'total_mensajes_cliente':  total_msjs,
            'filas_recalculadas':      recalculado,
            'tiempos_respuesta_total': cob['total'],
            'con_inicio_real':         cob['con_inicio_real'],
            'pct_cobertura':           round(cob['con_inicio_real'] / cob['total'] * 100, 1) if cob['total'] else 0,
            # offset ya viene avanzado por el loop; sumarle limit saltearia
            # una pagina entera de eventos sin procesar.
            'siguiente': (f'/backfill-mensajes?subdomain={subdomain}&offset={offset}'
                          if hay_mas else None),
            'listo': not hay_mas,
        })
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/backfill-salientes')
@token_requerido
def backfill_salientes():
    """Puebla 'mensajes_asesor' con los salientes que ya estan guardados dentro
    del JSON de 'eventos' con tipo_evento='desconocido'.

    Son los que llegaron ANTES de que el webhook supiera reconocerlos: cayeron
    en el cajon de "no reconocido" pero con el payload entero, asi que se
    recuperan sin pedirle nada a Kommo.

    Correr PRIMERO /backfill-mensajes: de ahi sale el mapa chat -> lead que
    necesitan los salientes que no traen el lead adentro (1 de cada 4). Si se
    corre al reves, esos quedan sin lead hasta que se vuelva a pasar.

    Paginado igual que /backfill-mensajes: cada llamada hace lo que alcanza en
    PRESUPUESTO_SEG y devuelve en 'siguiente' donde continuar."""
    subdomain = request.args.get('subdomain', '').strip()
    if subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain invalido', 'validos': list(PULSE_CONFIG)}), 400
    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0
    limit = 500
    PRESUPUESTO_SEG = 45

    # Por defecto solo el cajon viejo: lo que llega de ahora en adelante se
    # guarda como 'mensaje_saliente' y ya se inserto en vivo, asi que releerlo
    # es trabajo al pedo y ese conjunto crece todos los dias.
    #
    # Con ?incluir_vivos=1 se leen tambien esos. Sirve cuando cambia el parseo
    # y hay que corregir hacia atras lo ya guardado: paso con el nombre del
    # autor, que se guardaba con el &amp; de Kommo sin desescapar.
    tipos = ['desconocido']
    if request.args.get('incluir_vivos') == '1':
        tipos.append('mensaje_saliente')

    conn = get_conn()
    try:
        read_c  = conn.cursor(cursor_factory=RealDictCursor)
        write_c = conn.cursor()

        arranque  = time.monotonic()
        guardados = 0
        errores   = 0
        leidos    = 0
        hay_mas   = True
        while hay_mas and time.monotonic() - arranque < PRESUPUESTO_SEG:
            read_c.execute(
                "SELECT raw_data FROM eventos "
                "WHERE subdomain = %s AND tipo_evento = ANY(%s) "
                "AND raw_data LIKE %s "
                "ORDER BY id DESC LIMIT %s OFFSET %s",
                (subdomain, tipos, '%outgoing_message[add]%', limit, offset)
            )
            rows = read_c.fetchall()
            filas = []
            for row in rows:
                if not row['raw_data']:
                    continue
                try:
                    filas.extend(msjs_salientes(json.loads(row['raw_data'])))
                except Exception as e:
                    errores += 1
                    print(f"⚠️ Backfill salientes: {e}")
            guardados += insertar_msjs_asesor(write_c, subdomain, filas)
            leidos  += len(rows)
            offset  += len(rows)
            hay_mas  = len(rows) == limit

        completados = None
        if not hay_mas or request.args.get('completar') == '1':
            write_c.execute(SQL_COMPLETAR_LEAD_SALIENTE, (subdomain,))
            completados = write_c.rowcount
        conn.commit()

        read_c.execute(
            '''SELECT COUNT(*) AS total,
                      COUNT(lead_id) AS con_lead,
                      MIN(ts) AS desde, MAX(ts) AS hasta
                 FROM mensajes_asesor WHERE subdomain = %s''',
            (subdomain,)
        )
        cob = read_c.fetchone()
        # Quien manda los salientes. Es el dato que falta para decidir la regla
        # de bot vs persona, y conviene mirarlo sobre el total y no sobre una
        # muestra: 'WhatsApp Business' con user_id 0 puede ser el asesor
        # contestando desde la app.
        read_c.execute(
            '''SELECT autor_nombre, author_type, user_id, COUNT(*) AS n
                 FROM mensajes_asesor WHERE subdomain = %s
                GROUP BY autor_nombre, author_type, user_id
                ORDER BY n DESC LIMIT 25''',
            (subdomain,)
        )
        autores = [dict(r) for r in read_c.fetchall()]

        tz = PULSE_CONFIG.get(subdomain, {}).get('tz_offset', 0)
        def local(ts):
            if not ts:
                return None
            return (datetime.utcfromtimestamp(int(ts))
                    + timedelta(hours=tz)).strftime('%d/%m/%Y %H:%M')

        return jsonify({
            'offset':               offset,
            'eventos_procesados':   leidos,
            'segundos':             round(time.monotonic() - arranque, 1),
            'salientes_guardados':  guardados,
            'errores':              errores,
            'total_en_tabla':       cob['total'],
            'con_lead':             cob['con_lead'],
            'sin_lead':             cob['total'] - cob['con_lead'],
            'pct_con_lead':         round(cob['con_lead'] / cob['total'] * 100, 1) if cob['total'] else 0,
            'lead_completados_ahora': completados,
            'ventana':              {'desde': local(cob['desde']), 'hasta': local(cob['hasta'])},
            'quien_los_manda':      autores,
            # El flag viaja en 'siguiente': si se pierde, la segunda pagina deja
            # de leer los vivos y la reparacion queda a medias sin avisar. Es el
            # mismo error que tenia /backfill con el subdomain.
            'siguiente': (f'/backfill-salientes?subdomain={subdomain}&offset={offset}'
                          + ('&incluir_vivos=1' if 'mensaje_saliente' in tipos else '')
                          if hay_mas else None),
            'listo': not hay_mas,
        })
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/backfill-embudo')
@token_requerido
def backfill_embudo():
    """Completa leads_estado.pipeline_id desde el JSON de 'eventos'.

    El embudo llegaba en el 100% de los lead_update desde siempre y se tiraba;
    recien ahora se guarda. Sin este backfill el filtro por embudo solo veria
    los leads que se movieron despues del deploy, o sea casi ninguno.

    Solo toca pipeline_id, y solo donde esta vacio. Eso lo hace idempotente y
    ademas resuelve el orden: se lee de lo mas nuevo a lo mas viejo, asi que el
    primer valor que se escribe es el mas reciente y las paginas siguientes ya
    no lo pisan.

    A diferencia de /backfill, este no recalcula tiempos de respuesta ni toca
    ninguna otra columna: es una pasada liviana para una sola cosa."""
    subdomain = request.args.get('subdomain', '').strip()
    if subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain invalido', 'validos': list(PULSE_CONFIG)}), 400
    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0
    limit = 1000
    PRESUPUESTO_SEG = 45

    conn = get_conn()
    try:
        read_c  = conn.cursor(cursor_factory=RealDictCursor)
        write_c = conn.cursor()

        arranque = time.monotonic()
        leidos = errores = 0
        hay_mas = True
        # Cuantos habia ANTES, para poder decir cuantos se completaron en esta
        # corrida. No se usa cursor.rowcount: execute_values parte el INSERT en
        # paginas internas y rowcount refleja solo la ultima, asi que el numero
        # sale mucho mas chico que la realidad.
        read_c.execute('SELECT COUNT(pipeline_id) AS n FROM leads_estado '
                       'WHERE subdomain = %s', (subdomain,))
        antes = read_c.fetchone()['n']
        while hay_mas and time.monotonic() - arranque < PRESUPUESTO_SEG:
            read_c.execute(
                "SELECT lead_id, raw_data FROM eventos "
                "WHERE subdomain = %s AND tipo_evento = 'lead_update' "
                "ORDER BY id DESC LIMIT %s OFFSET %s",
                (subdomain, limit, offset))
            rows = read_c.fetchall()

            # Un lead puede aparecer muchas veces en la pagina; alcanza con el
            # primero, que por el ORDER BY id DESC es el mas reciente.
            pares = {}
            for row in rows:
                if not row['raw_data'] or row['lead_id'] in pares:
                    continue
                try:
                    data = json.loads(row['raw_data'])
                    prefix = prefix_de_lead(data, row['lead_id'])
                    if prefix is None:
                        continue
                    pid = data.get(f'{prefix}[pipeline_id]')
                    if pid:
                        pares[row['lead_id']] = int(pid)
                except Exception as e:
                    errores += 1
                    print(f'⚠️ Backfill embudo: {e}')

            if pares:
                execute_values(
                    write_c,
                    'UPDATE leads_estado le SET pipeline_id = v.pid '
                    'FROM (VALUES %s) AS v(sub, lead_id, pid) '
                    'WHERE le.subdomain = v.sub AND le.lead_id = v.lead_id '
                    '  AND le.pipeline_id IS NULL',
                    [(subdomain, lid, pid) for lid, pid in pares.items()],
                    template='(%s, %s, %s::bigint)')

            leidos  += len(rows)
            offset  += len(rows)
            hay_mas  = len(rows) == limit
        conn.commit()

        read_c.execute(
            'SELECT COUNT(*) AS total, COUNT(pipeline_id) AS con_embudo '
            'FROM leads_estado WHERE subdomain = %s', (subdomain,))
        cob = read_c.fetchone()
        read_c.execute(
            'SELECT pipeline_id, COUNT(*) AS leads FROM leads_estado '
            'WHERE subdomain = %s AND pipeline_id IS NOT NULL '
            'GROUP BY pipeline_id ORDER BY leads DESC LIMIT 30', (subdomain,))
        por_embudo = [
            {'pipeline_id': r['pipeline_id'],
             'nombre': nombre_embudo(subdomain, r['pipeline_id']),
             'leads': r['leads']}
            for r in read_c.fetchall()
        ]

        return jsonify({
            'offset':            offset,
            'eventos_leidos':    leidos,
            'segundos':          round(time.monotonic() - arranque, 1),
            'leads_completados': cob['con_embudo'] - antes,
            'errores':           errores,
            'leads_totales':     cob['total'],
            'con_embudo':        cob['con_embudo'],
            'pct':               round(cob['con_embudo'] / cob['total'] * 100, 1) if cob['total'] else 0,
            'por_embudo':        por_embudo,
            'siguiente': (f'/backfill-embudo?subdomain={subdomain}&offset={offset}'
                          if hay_mas else None),
            'listo': not hay_mas,
        })
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/health/carga')
def health_carga():
    """Cuantos hilos llega a tener el servidor. Es la prueba que falta.

    Cada webhook lanza un hilo nuevo sin limite. La hipotesis de por que Kommo
    desactiva los webhooks es que en las rafagas —una operacion masiva en el CRM
    dispara cientos de POST juntos— se crean cientos de hilos, se agotan las
    conexiones a Supabase y las respuestas se vuelven lentas.

    Como leerlo:
      hilos_maximo cerca de 10-20  -> la hipotesis se cae, buscar en otro lado
      hilos_maximo en cientos      -> confirmada, corresponde poner una cola

    Los contadores se reinician con cada deploy o reinicio del servicio."""
    return jsonify({
        'hilos_ahora':                threading.active_count(),
        'hilos_maximo':               _CARGA['hilos_maximo'],
        'cuando_el_maximo':           _CARGA['cuando_el_maximo'],
        'webhooks_desde_el_arranque': _CARGA['webhooks'],
        'arranque':                   _CARGA['arranque'],
        # Kommo desactiva el webhook con el mensaje "respuesta no valida", asi
        # que lo que importa no es solo el pico de hilos sino si alguna vez le
        # respondimos distinto de 200. Estos dos son las unicas formas conocidas
        # en que este endpoint puede fallar; si siguen en 0 despues de una
        # desactivacion, la causa esta fuera de la aplicacion.
        'hilos_rechazados':           _CARGA['hilos_rechazados'],
        'errores_body':               _CARGA['errores_body'],
        # Lo PRIMERO que hay que mirar. Recibir no es guardar: el 31/08 se
        # recibieron 65.000 webhooks y no se guardo ninguno, y este endpoint
        # decia que todo estaba bien porque solo miraba las lineas de arriba.
        'guardando':                  _CARGA['escrituras_fallidas'] == 0,
        'escrituras_ok':              _CARGA['escrituras_ok'],
        'escrituras_fallidas':        _CARGA['escrituras_fallidas'],
        'ultimo_error_escritura':     _CARGA['ultimo_error_escritura'],
        'cuando_el_ultimo_error':     _CARGA['cuando_el_ultimo_error'],
        'eventos_no_guardados':       _CARGA['eventos_no_guardados'],
        'formas_desconocidas_vistas': len(_FORMAS_VISTAS),
        'como_leerlo': 'guardando:false es la alarma que importa — llegan '
                       'webhooks y no se estan guardando; el motivo exacto '
                       'esta en ultimo_error_escritura. '
                       'hilos_maximo en cientos confirma que las rafagas '
                       'saturan el servidor; cerca de 10-20 lo descarta. '
                       'hilos_rechazados o errores_body arriba de 0 explican '
                       'las desactivaciones de Kommo.',
    })

@app.route('/ping')
def ping():
    """Responde sin tocar la base: sirve para saber si el servidor esta vivo
    aunque la base este lenta o caida.

    Util para un monitor externo (UptimeRobot y similares) que avise cuando el
    servicio deja de responder, que es cuando Kommo empieza a contar fallos y
    termina desactivando el webhook."""
    return jsonify({'ok': True, 'ts': datetime.utcnow().isoformat()})

# Margen desde la apertura antes de que el dia de hoy cuente como hueco: a
# primera hora todavia no tiene por que haber llegado nada.
GRACIA_APERTURA_HORAS = 3

# Sin recibir nada por mas de esto, se da la cuenta por caida. Cubre una noche
# entera y un fin de semana corto sin dar falsas alarmas.
HORAS_PARA_DARLO_POR_CAIDO = 20

@app.route('/health/actividad')
def health_actividad():
    """Detecta huecos en la captura: dias sin un solo evento recibido.

    Si el webhook de Kommo se desactiva dejamos de recibir mensajes en
    silencio. Nada falla, no hay error en los logs: simplemente no llega nada,
    y el dashboard sigue mostrando numeros como si todo estuviera bien. Peor:
    los leads de esos dias quedan sin sus mensajes, asi que la espera real no
    se puede reconstruir y el contador de 'sin responder' los ignora.

    Un dia laborable con cero eventos es la senal de que el webhook se cayo."""
    try:
        dias = min(int(request.args.get('dias', 45)), 180)
    except ValueError:
        dias = 45
    desde = (datetime.utcnow() - timedelta(days=dias)).strftime('%Y-%m-%d')
    hoy = datetime.utcnow().date()

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute(
            '''SELECT subdomain,
                      LEFT(capturado_at, 10) AS dia,
                      COUNT(*)               AS eventos
               FROM eventos
               WHERE capturado_at >= %s
               GROUP BY 1, 2''',
            (desde,)
        )
        filas = c.fetchall()
        c.execute('SELECT subdomain, MAX(capturado_at) AS ultimo FROM eventos GROUP BY 1')
        ultimos = {r['subdomain']: r['ultimo'] for r in c.fetchall()}
    finally:
        conn.close()

    por_sub = defaultdict(dict)
    for f in filas:
        por_sub[f['subdomain']][f['dia']] = f['eventos']

    resultado = {}
    for subdomain in PULSE_CONFIG:
        conteo = por_sub.get(subdomain, {})
        if not conteo:
            resultado[subdomain] = {'sin_datos': True}
            continue
        primero  = datetime.strptime(min(conteo), '%Y-%m-%d').date()
        cfg_sub  = PULSE_CONFIG[subdomain]
        dias_lab = cfg_sub['dias_laborables']
        # Hora local del cliente, para saber si el dia de hoy ya arranco.
        hora_local = (datetime.utcnow() + timedelta(hours=cfg_sub['tz_offset'])).hour
        apertura   = cfg_sub['horario'][0]

        huecos = []
        d = primero
        # <= hoy, no < hoy: antes el bucle cortaba ayer y una caida que empezaba
        # un viernes y seguia el lunes daba CERO huecos, porque el unico dia
        # laborable vacio era justamente hoy. Paso en gruporegalado: 78 horas sin
        # recibir nada y el endpoint respondia ok:true.
        while d <= hoy:
            # Solo alarman los dias laborables: un domingo sin eventos es normal.
            if d.weekday() in dias_lab and conteo.get(d.strftime('%Y-%m-%d'), 0) == 0:
                # Hoy recien cuenta pasado un margen desde la apertura: a primera
                # hora todavia no tiene por que haber llegado nada.
                if d < hoy or hora_local >= apertura + GRACIA_APERTURA_HORAS:
                    huecos.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

        ultimo = ultimos.get(subdomain)
        horas_sin_recibir = None
        if ultimo:
            try:
                horas_sin_recibir = round(
                    (datetime.utcnow() - datetime.fromisoformat(ultimo)).total_seconds() / 3600, 1)
            except ValueError:
                pass

        # 'recibiendo' mira el AHORA; los huecos son historia. Antes se mezclaban
        # en un solo 'ok' y una cuenta que se cayo hace cinco dias y ya se
        # reconecto seguia figurando en false, con horas_sin_recibir en 0.0 al
        # lado. Eso llevo a reconectar un webhook que estaba funcionando bien.
        recibiendo = (
            horas_sin_recibir is not None
            and horas_sin_recibir <= HORAS_PARA_DARLO_POR_CAIDO
        )
        resultado[subdomain] = {
            'recibiendo':         recibiendo,
            'ultimo_evento':      ultimo,
            'horas_sin_recibir':  horas_sin_recibir,
            'huecos_historicos':  len(huecos),
            'dias_laborables_sin_un_solo_evento': huecos,
            'dias_con_datos':     len(conteo),
            'eventos_ultimos_7d': sum(
                n for dia, n in conteo.items()
                if dia >= (hoy - timedelta(days=7)).strftime('%Y-%m-%d')
            ),
        }

    caidas = [s for s, r in resultado.items() if not r.get('recibiendo')]
    return jsonify({
        # Lo unico que hay que mirar para saber si algo esta roto AHORA.
        'todo_recibiendo': not caidas,
        'caidas_ahora':    caidas,
        'ventana_dias':    dias,
        'subdominios':     resultado,
    })

@app.route('/health/alta')
@token_requerido
def health_alta():
    """Herramienta para dar de alta una cuenta nueva.

    Sin ?subdomain: lista TODOS los subdominios que estan mandando eventos,
    incluidos los que todavia no estan en PULSE_CONFIG. Sirve para confirmar
    que el webhook de una cuenta nueva ya esta conectado.

    Con ?subdomain=X: lista los nombres de campos custom que realmente llegan,
    con un valor de ejemplo. Cada cuenta de Kommo nombra distinto sus campos de
    fecha de ultimo mensaje ('F Ult msj cliente', 'F ult msj cliente seg',
    'F utl msj cliente'...), asi que hay que leerlos en vez de adivinarlos: un
    nombre mal configurado no da error, simplemente no mide nada.

    Los campos cuyo valor parece un epoch de 10 digitos son los candidatos."""
    subdomain = request.args.get('subdomain', '').strip()
    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)

        if not subdomain:
            c.execute('''SELECT subdomain,
                                COUNT(*)              AS eventos,
                                MAX(capturado_at)     AS ultimo
                         FROM eventos GROUP BY 1 ORDER BY 2 DESC''')
            filas = [dict(r) for r in c.fetchall()]
            for f in filas:
                f['configurado_en_pulse'] = f['subdomain'] in PULSE_CONFIG
            return jsonify({
                'subdominios': filas,
                'como_seguir': 'Volve a llamar con ?subdomain=<nombre> para ver sus campos custom.',
            })

        c.execute(
            "SELECT raw_data FROM eventos "
            "WHERE subdomain = %s AND tipo_evento = 'lead_update' "
            "ORDER BY id DESC LIMIT 300",
            (subdomain,)
        )
        rows = c.fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({
            'subdomain': subdomain,
            'error': 'No hay eventos lead_update de este subdominio. '
                     'Revisa que el webhook este conectado en Kommo.',
        }), 404

    vistos = {}
    for row in rows:
        if not row['raw_data']:
            continue
        try:
            data = json.loads(row['raw_data'])
        except (ValueError, TypeError):
            continue
        for i in get_batch_indices(data, 'leads[update]'):
            pref = f'leads[update][{i}]'
            for j in get_batch_indices(data, f'{pref}[custom_fields]'):
                nombre = data.get(f'{pref}[custom_fields][{j}][name]')
                valor  = data.get(f'{pref}[custom_fields][{j}][values][0]')
                if not nombre:
                    continue
                v = vistos.setdefault(nombre, {'campo': nombre, 'veces': 0, 'ejemplo': None,
                                               'parece_fecha': False})
                v['veces'] += 1
                if valor and v['ejemplo'] is None:
                    v['ejemplo'] = str(valor)[:40]
                    # Un epoch de 10 digitos es lo que guardan los campos de fecha.
                    if str(valor).isdigit() and len(str(valor)) == 10:
                        v['parece_fecha'] = True
                        try:
                            v['ejemplo_legible'] = datetime.utcfromtimestamp(int(valor)).strftime('%Y-%m-%d %H:%M UTC')
                        except (ValueError, OSError):
                            pass

    campos = sorted(vistos.values(), key=lambda x: (not x['parece_fecha'], -x['veces']))
    return jsonify({
        'subdomain':           subdomain,
        'configurado_en_pulse': subdomain in PULSE_CONFIG,
        'eventos_revisados':   len(rows),
        'campos_encontrados':  len(campos),
        'candidatos_a_fecha':  [c for c in campos if c['parece_fecha']],
        'todos_los_campos':    campos,
    })

@app.route('/health/desconocidos')
@token_requerido
def health_desconocidos():
    """Que traen los POST que el webhook recibe y no reconoce.

    Se guardan con tipo_evento='desconocido'. Este endpoint agrupa sus claves
    por PATRON (los indices se reemplazan por [n]) para ver de que tipo de
    evento se trata, con un valor de ejemplo de cada una.

    La pregunta que buscamos responder: si alguno de estos es el mensaje
    SALIENTE del asesor. Todo lo que falta para medir bien —la hora exacta de
    la respuesta y quien contesto— depende de ese dato, y hasta ahora se dio por
    hecho que Kommo no lo manda.

    OJO con leer las cantidades como volumen. Desde el arreglo del disco
    (31/08/2026) solo se guardan los primeros EJEMPLOS_POR_FORMA de cada forma:
    'veces' dice cuantos EJEMPLOS tenemos, no cuantos llegaron. El total que
    llega y se descarta esta en /health/carga, en eventos_no_guardados."""
    subdomain = request.args.get('subdomain', '').strip()
    try:
        limite = min(int(request.args.get('limite', 300)), 3000)
    except ValueError:
        limite = 300

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        q = "SELECT subdomain, raw_data FROM eventos WHERE tipo_evento = 'desconocido'"
        params = []
        if subdomain:
            q += ' AND subdomain = %s'
            params.append(subdomain)
        q += ' ORDER BY id DESC LIMIT %s'
        params.append(limite)
        c.execute(q, params)
        rows = c.fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({
            'payloads': 0,
            'nota': 'Todavia no se guardo ninguno. Empiezan a guardarse recien '
                    'desde este deploy; antes solo se logueaban y se perdian.',
        })

    patrones = defaultdict(lambda: {'veces': 0, 'ejemplo': None})
    por_sub  = defaultdict(int)
    firmas   = defaultdict(int)
    for row in rows:
        por_sub[row['subdomain']] += 1
        if not row['raw_data']:
            continue
        try:
            data = json.loads(row['raw_data'])
        except (ValueError, TypeError):
            continue
        claves = set()
        for k, v in data.items():
            p = re.sub(r'\[\d+\]', '[n]', k)
            claves.add(p)
            e = patrones[p]
            e['veces'] += 1
            if e['ejemplo'] is None and v not in (None, ''):
                e['ejemplo'] = str(v)[:60]
        # La firma (raiz de las claves) dice de que tipo de evento se trata.
        raices = sorted({p.split('[')[0] for p in claves})
        firmas['+'.join(raices)] += 1

    orden = sorted(patrones.items(), key=lambda kv: -kv[1]['veces'])
    return jsonify({
        'payloads':            len(rows),
        'por_subdominio':      dict(por_sub),
        'tipos_de_evento':     dict(sorted(firmas.items(), key=lambda kv: -kv[1])),
        'hay_mensajes':        any('message' in p for p in patrones),
        'claves': [
            {'patron': p, 'veces': d['veces'], 'ejemplo': d['ejemplo']}
            for p, d in orden[:60]
        ],
    })

@app.route('/health/primer-evento')
def health_primer_evento():
    """Muestra el evento con el timestamp mas antiguo por subdominio,
    crudo, para verificar que 'fecha_minima' no este mal calculada."""
    subdomain = request.args.get('subdomain', '').strip()
    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        query = 'SELECT id, subdomain, tipo_evento, lead_id, timestamp, capturado_at FROM eventos WHERE timestamp IS NOT NULL'
        params = []
        if subdomain:
            query += ' AND subdomain = %s'
            params.append(subdomain)
        query += ' ORDER BY timestamp ASC LIMIT 5'
        c.execute(query, params)
        rows = [dict(r) for r in c.fetchall()]
    finally:
        conn.close()
    for r in rows:
        r['timestamp_fecha_utc'] = datetime.utcfromtimestamp(r['timestamp']).isoformat() if r['timestamp'] else None
    return jsonify(rows)

@app.route('/health/campos')
def health_campos():
    """Verifica que los nombres de campo custom configurados por cliente
    (PULSE_CONFIG[...]['campos']) realmente aparezcan en eventos recientes.
    Sin esto, un typo o un cliente renombrando un campo en Kommo pasa
    desapercibido hasta que alguien nota un numero raro en el dashboard."""
    resultados = {}
    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        for subdomain in PULSE_CONFIG:
            campos = campos_de(subdomain)
            c.execute(
                "SELECT raw_data FROM eventos WHERE subdomain = %s AND tipo_evento = 'lead_update' ORDER BY id DESC LIMIT 200",
                (subdomain,)
            )
            rows = c.fetchall()
            ok_cliente = False
            ok_asesor = False
            for row in rows:
                if not row['raw_data']:
                    continue
                data = json.loads(row['raw_data'])
                # Revisar todos los leads del batch, no solo el [0]: si el
                # primero no trae los campos, mirar solo ese indice reporta
                # "campo no encontrado" cuando en realidad si esta llegando.
                for i in get_batch_indices(data, 'leads[update]'):
                    prefix = f'leads[update][{i}]'
                    if not ok_cliente and get_custom_field(data, prefix, campos['cliente']) is not None:
                        ok_cliente = True
                    if not ok_asesor and get_custom_field(data, prefix, campos['asesor']) is not None:
                        ok_asesor = True
                if ok_cliente and ok_asesor:
                    break
            c.execute(
                '''SELECT COUNT(*) AS total, COUNT(DISTINCT (lead_id, f_ult_msj_cliente)) AS distintos
                   FROM tiempos_respuesta WHERE subdomain = %s''',
                (subdomain,)
            )
            dup = c.fetchone()
            resultados[subdomain] = {
                'eventos_revisados': len(rows),
                'campo_cliente': campos['cliente'],
                'campo_cliente_encontrado': ok_cliente,
                'campo_asesor': campos['asesor'],
                'campo_asesor_encontrado': ok_asesor,
                'fecha_minima': _fecha_minima(subdomain),
                'tiempos_respuesta_total': dup['total'],
                'tiempos_respuesta_distintos': dup['distintos'],
                'duplicados_por_rafaga': dup['total'] - dup['distintos'],
                'ok': ok_cliente and ok_asesor,
            }
    finally:
        conn.close()
    return jsonify({
        'ok': all(r['ok'] for r in resultados.values()),
        'subdominios': resultados,
    })

def _pctl(valores, p):
    """Percentil por rango mas cercano. Suficiente para un diagnostico;
    no interpola como el percentil 'exacto'."""
    if not valores:
        return 0
    s = sorted(valores)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]

def _reconstruir_turnos(msjs_cliente, respuestas, tz_offset, h_ini, h_fin, dias_lab):
    """Arma los turnos cliente->asesor y compara, para cada uno, la espera REAL
    (desde el PRIMER mensaje del cliente sin responder) contra lo que mide Pulse
    hoy (desde el ULTIMO mensaje antes de la respuesta).

    Kommo solo manda webhook de los mensajes ENTRANTES, asi que la hora de la
    respuesta del asesor no esta en 'eventos'. Se toma de f_ult_msj_asesor en
    'tiempos_respuesta', que es justamente el campo que alimenta el dashboard.

    Para cada respuesta del asesor, la espera empezo en el primer mensaje del
    cliente posterior a la respuesta ANTERIOR de ese lead. Ambas medidas usan
    tiempo efectivo (descontando horas no laborables) y se descartan las
    reactivaciones, para que sean comparables con /pulse/data.

    Solo se miden las respuestas POSTERIORES al primer mensaje que capturamos
    de ese lead. Antes de eso no estabamos escuchando, asi que no se puede
    saber si habia alguien esperando; contarlas como "el asesor respondio al
    vacio" inventa un problema que no existe.

    Devuelve (turnos, descartes), donde descartes separa los motivos en vez de
    sumarlos en un solo numero: son causas distintas y llevan a acciones
    opuestas.

    msjs_cliente: iterable de {'lead_id', 'ts'} (solo entrantes)
    respuestas:   iterable de {'lead_id', 'ts', 'inicio_pulse'}, donde
                  inicio_pulse es desde cuando mide el dashboard hoy"""
    msgs_por_lead = defaultdict(list)
    for m in msjs_cliente:
        if m['lead_id'] and m['ts']:
            msgs_por_lead[str(m['lead_id'])].append(m['ts'])

    # Se guarda tambien desde cuando mide el dashboard para cada respuesta, que
    # es lo unico contra lo que tiene sentido compararse.
    resp_por_lead = defaultdict(dict)
    for r in respuestas:
        if r['lead_id'] and r['ts']:
            resp_por_lead[str(r['lead_id'])][r['ts']] = r.get('inicio_pulse')

    turnos = []
    descartes = Counter()
    for lead_id, resp in resp_por_lead.items():
        msgs = sorted(set(msgs_por_lead.get(lead_id, ())))
        if not msgs:
            # Nunca capturamos un mensaje de este lead. No es que el asesor
            # respondiera al vacio: es que no estabamos escuchando.
            descartes['lead_sin_mensajes_capturados'] += len(resp)
            continue

        piso = msgs[0]
        anterior = None
        for a in sorted(resp):
            if a < piso:
                # La regla de Juan: una espera que empezo antes de nuestro
                # primer mensaje de ese lead no se puede medir ni juzgar.
                # Contarla como "nadie esperaba" inventaba un problema que no
                # existe, y era el grueso del hueco en gruporegalado.
                descartes['antes_de_nuestro_primer_mensaje'] += 1
                continue

            lo = bisect.bisect_right(msgs, anterior) if anterior is not None else 0
            # bisect_right y no left: Kommo redondea estos campos a minutos, asi
            # que un mensaje y una respuesta del mismo minuto no se pueden
            # ordenar. Dejarlo afuera lo contaba como "nadie esperaba"; contarlo
            # como espera de casi cero se acerca mas a lo que paso.
            hi = bisect.bisect_right(msgs, a)
            anterior = a
            if lo >= hi:
                # Aca si: el asesor escribio sin que hubiera nadie esperando.
                descartes['sin_nadie_esperando'] += 1
                continue

            primero, ultimo = msgs[lo], msgs[hi - 1]
            if a - primero > GAP_REACTIVACION_SEG:
                descartes['reactivaciones'] += 1
                continue

            real = _calc_tiempo_efectivo(primero, a, tz_offset, h_ini, h_fin, dias_lab)
            # Lo que muestra el dashboard HOY. Sale de LEAST(f_primer_msj_cliente,
            # f_ult_msj_cliente): donde el backfill calculo el primer mensaje sin
            # responder, ya mide lo mismo que 'real'. Antes esto se calculaba
            # siempre desde el ULTIMO mensaje, o sea contra una version del
            # calculo que ya no existe, y el endpoint reportaba como pendiente
            # un sesgo que estaba corregido.
            # None = el dashboard no muestra esta respuesta (no paso sus
            # filtros). Se mide igual la espera real, pero no se la compara
            # contra un numero que nadie ve.
            inicio_pulse = resp[a]
            pulse = (_calc_tiempo_efectivo(inicio_pulse, a, tz_offset, h_ini, h_fin, dias_lab)
                     if inicio_pulse else None)
            turnos.append({
                'lead_id':      lead_id,
                'msjs_cliente': hi - lo,
                'real_seg':     real,
                'pulse_seg':    pulse,
                'sesgo_seg':    (real - pulse) if pulse is not None else None,
            })
    return turnos, dict(descartes)

def _turnos_de_mensajes(entrantes, salientes, autos=None):
    """Arma los turnos cliente -> respuesta a partir de los mensajes REALES.

    Un turno empieza con el primer mensaje del cliente que quedo sin contestar
    y termina con el primer mensaje saliente posterior. Los mensajes que el
    cliente encadena mientras espera NO reinician el reloj: si escribe 10:00,
    10:05 y 10:10 y le contestan 10:12, espero 12 minutos, no 2.

    Es la diferencia de fondo con el calculo viejo. 'F ult msj cliente' guarda
    el ULTIMO mensaje, asi que medir desde ahi subestima la espera; y como cada
    respuesta pisa la anterior, de un lead con 7 respuestas quedaba 1 sola.

    NO se decide si el que contesto es un bot o una persona: se devuelve el
    nombre tal cual y cada uno es una fila del ranking. Eso evita tener que
    adivinar que es 'WhatsApp Business', que son 538 mensajes en tucoytico.

    entrantes: [(lead_id, ts)]   salientes: [(lead_id, ts, autor, user_id)]

    'auto' marca los turnos que cerro un bot. No se descartan aca: el ranking
    los muestra como una fila propia, que es como se pidio. Los descarta despues
    quien calcula el numero grande."""
    autos = autos or set()
    por_lead = defaultdict(lambda: {'ent': [], 'sal': []})
    for lead_id, ts in entrantes:
        por_lead[lead_id]['ent'].append(ts)
    # El 5o campo es el id del mensaje. Sirve para armar el link que abre la
    # conversacion EN esa respuesta y no al final. Los que llaman sin el siguen
    # andando: el link se degrada al lead, como antes.
    for fila in salientes:
        lead_id, ts, autor, uid = fila[:4]
        msg_id = fila[4] if len(fila) > 4 else None
        por_lead[lead_id]['sal'].append((ts, autor, uid, msg_id))

    turnos = []
    for lead_id, m in por_lead.items():
        if not m['sal']:
            continue
        ent = sorted(m['ent'])
        sal = sorted(m['sal'])
        # Piso de captura: una respuesta anterior al primer mensaje que
        # guardamos contesta algo que nunca vimos. Medirla daria una espera
        # inventada, asi que se descarta.
        if not ent:
            continue
        piso = ent[0]
        i = 0
        esperando = None
        for ts_sal, autor, uid, msg_id in sal:
            if ts_sal < piso:
                continue
            while i < len(ent) and ent[i] <= ts_sal:
                if esperando is None:
                    esperando = ent[i]
                i += 1
            if esperando is None:
                continue          # respuesta sin mensaje previo del cliente
            turnos.append({
                'lead_id': lead_id,
                'inicio':  esperando,
                'fin':     ts_sal,
                'seg':     ts_sal - esperando,
                # Normalizado contra nuestra lista para que el mismo asesor no
                # aparezca dos veces por una tilde. Ver nombre_de_remitente.
                'autor':   nombre_de_remitente(uid, autor) or f'usuario {uid}',
                'user_id': uid,
                # 'auto' se decide con el nombre ORIGINAL del mensaje: es el
                # que figura en REMITENTES_AUTOMATICOS.
                'auto':    (autor or '') in autos,
                # El mensaje que CERRO la espera. Con esto el link del tablero
                # abre la conversacion parada en esa respuesta.
                'msg_id':  msg_id,
            })
            esperando = None
    return turnos

@app.route('/health/comparar-calculo')
@token_requerido
def health_comparar_calculo():
    """Compara el calculo VIEJO contra el NUEVO sobre el mismo periodo.

    El viejo sale de los campos de fecha de Kommo ('F ult msj cliente' /
    'F ult msj enviado'), que guardan un solo valor y se truncan a minutos.
    El nuevo sale de los mensajes que atrapa el webhook, con segundos exactos
    y todos los turnos, no solo el ultimo.

    Se mide antes de cambiar el dashboard porque los numeros se van a mover y
    hay que poder explicar cuanto y por que. Comparar en segundos crudos, que
    es lo que guarda 'tiempos_respuesta': el descuento de horario laboral se
    aplica despues y por igual a los dos, asi que no afecta la comparacion.

    La ventana arranca donde arrancan los salientes de esa cuenta: antes de eso
    el metodo nuevo no tiene con que medir.

    Solo lee."""
    subdomain = request.args.get('subdomain', '').strip()
    if subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain invalido', 'validos': list(PULSE_CONFIG)}), 400

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute('SELECT MIN(ts) AS desde FROM mensajes_asesor WHERE subdomain = %s',
                  (subdomain,))
        fila = c.fetchone()
        desde = fila['desde'] if fila else None
        if not desde:
            return jsonify({
                'subdomain': subdomain,
                'error': 'esta cuenta no tiene mensajes salientes todavia',
                'como_arreglarlo': 'suscribirla a add_outgoing_message con '
                                   'scripts/suscribir_salientes.py',
            })
        try:
            desde = int(request.args.get('desde', desde))
        except ValueError:
            pass

        c.execute('SELECT lead_id, ts FROM mensajes_cliente '
                  'WHERE subdomain = %s AND ts >= %s', (subdomain, desde))
        entrantes = [(r['lead_id'], r['ts']) for r in c.fetchall()]

        c.execute('SELECT lead_id, ts, autor_nombre, user_id FROM mensajes_asesor '
                  'WHERE subdomain = %s AND ts >= %s AND lead_id IS NOT NULL',
                  (subdomain, desde))
        salientes = [(r['lead_id'], r['ts'], r['autor_nombre'], r['user_id'])
                     for r in c.fetchall()]

        # El calculo viejo, acotado a la MISMA ventana.
        c.execute('SELECT lead_id, tiempo_respuesta_seg FROM tiempos_respuesta '
                  'WHERE subdomain = %s AND f_ult_msj_asesor >= %s '
                  'AND tiempo_respuesta_seg IS NOT NULL', (subdomain, desde))
        viejo = [dict(r) for r in c.fetchall()]
    finally:
        conn.close()

    turnos = _turnos_de_mensajes(entrantes, salientes, remitentes_auto_de(subdomain))

    def resumen(segs):
        segs = sorted(s for s in segs if s is not None and s >= 0)
        if not segs:
            return None
        return {
            'n':       len(segs),
            'mediana': _pctl(segs, 50),
            'p90':     _pctl(segs, 90),
            'maximo':  segs[-1],
        }

    # Por quien contesto. Esta es la vista que pidio el usuario: el bot no se
    # filtra, se muestra como una fila mas y se ve su volumen y su velocidad.
    por_autor = defaultdict(list)
    for t in turnos:
        por_autor[t['autor']].append(t['seg'])
    ranking = sorted(
        ({'autor': a, **(resumen(s) or {})} for a, s in por_autor.items()),
        key=lambda r: -(r.get('n') or 0))

    tz = PULSE_CONFIG.get(subdomain, {}).get('tz_offset', 0)
    return jsonify({
        'subdomain': subdomain,
        'ventana_desde': (datetime.utcfromtimestamp(desde)
                          + timedelta(hours=tz)).strftime('%d/%m/%Y %H:%M'),
        'metodo_viejo_campos_de_fecha': {
            **(resumen([r['tiempo_respuesta_seg'] for r in viejo]) or {}),
            'leads': len({r['lead_id'] for r in viejo}),
        },
        'metodo_nuevo_mensajes_reales': {
            **(resumen([t['seg'] for t in turnos]) or {}),
            'leads': len({t['lead_id'] for t in turnos}),
        },
        # Lo mismo sacando al bot que saluda, para ver cuanto lo arrastra hacia
        # abajo. Es la decision que hay que tomar sobre el numero grande.
        'metodo_nuevo_solo_personas': resumen(
            [t['seg'] for t in turnos if not t['auto']]),
        'remitentes_tratados_como_bot': sorted(remitentes_auto_de(subdomain)),
        'quien_contesto': ranking,
        'nota': 'Segundos crudos, sin descuento de horario laboral: se aplica '
                'igual a los dos metodos y no cambia la comparacion.',
    })

@app.route('/health/inventario')
@token_requerido
def health_inventario():
    """Que capturamos de verdad, por cuenta: que manda Kommo y que guardamos.

    Son dos cosas distintas y conviene verlas juntas. Kommo manda decenas de
    claves por evento; de esas usamos unas pocas, y el resto queda solo dentro
    del JSON de 'eventos'. Sin este inventario la unica forma de saberlo es
    leer el codigo.

    Tres secciones:

      llega_de_kommo   las claves de cada tipo de evento, con los indices
                       normalizados a [n] y un valor de ejemplo. Sale de los
                       payloads reales de ESA cuenta, no de la documentacion:
                       cada cuenta nombra distinto sus campos custom y activa
                       distintos canales.
      guardamos        las columnas de nuestras tablas, leidas del catalogo de
                       Postgres para que no se desactualicen con el codigo.
      un_lead          un lead concreto con todo lo que tenemos de el. Si no se
                       pasa lead_id se elige el ultimo que haya respondido un
                       asesor, que es el caso que interesa mirar.

    Solo lee."""
    subdomain = request.args.get('subdomain', '').strip()
    if subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain invalido', 'validos': list(PULSE_CONFIG)}), 400
    lead_id = request.args.get('lead_id', '').strip()
    try:
        por_tipo = min(int(request.args.get('muestra', 120)), 400)
    except ValueError:
        por_tipo = 120

    tz = PULSE_CONFIG[subdomain].get('tz_offset', 0)
    def local(ts):
        try:
            return (datetime.utcfromtimestamp(int(ts))
                    + timedelta(hours=tz)).strftime('%d/%m/%Y %H:%M:%S')
        except (TypeError, ValueError, OSError):
            return None

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)

        # --- 1. Que llega de Kommo -------------------------------------------
        c.execute('SELECT tipo_evento, COUNT(*) AS n FROM eventos '
                  'WHERE subdomain = %s GROUP BY tipo_evento ORDER BY n DESC',
                  (subdomain,))
        totales = {r['tipo_evento']: r['n'] for r in c.fetchall()}

        llega = {}
        for tipo in totales:
            c.execute("SELECT raw_data FROM eventos WHERE subdomain = %s "
                      "AND tipo_evento = %s ORDER BY id DESC LIMIT %s",
                      (subdomain, tipo, por_tipo))
            patrones = defaultdict(lambda: {'veces': 0, 'ejemplo': None})
            leidos = 0
            for row in c.fetchall():
                try:
                    data = json.loads(row['raw_data'] or '{}')
                except (ValueError, TypeError):
                    continue
                leidos += 1
                for k, v in data.items():
                    p = re.sub(r'\[\d+\]', '[n]', k)
                    e = patrones[p]
                    e['veces'] += 1
                    if e['ejemplo'] is None and v not in (None, ''):
                        e['ejemplo'] = str(v)[:70]
            llega[tipo] = {
                'eventos_guardados': totales[tipo],
                'payloads_leidos':   leidos,
                'claves': [
                    {'campo': p, 'en_payloads': d['veces'], 'ejemplo': d['ejemplo']}
                    for p, d in sorted(patrones.items(), key=lambda kv: -kv[1]['veces'])
                ],
            }

        # --- 2. Que guardamos nosotros ---------------------------------------
        # Del catalogo de Postgres y no de una lista escrita a mano: una lista
        # escrita a mano se desactualiza en el primer ALTER TABLE.
        c.execute("""
            SELECT table_name, column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name IN ('eventos','tiempos_respuesta','mensajes_cliente',
                                  'leads_estado','mensajes_asesor','conversaciones')
             ORDER BY table_name, ordinal_position
        """)
        guardamos = defaultdict(list)
        for r in c.fetchall():
            guardamos[r['table_name']].append(f"{r['column_name']} ({r['data_type']})")

        c.execute("""
            SELECT 'tiempos_respuesta' AS t, COUNT(*) AS n FROM tiempos_respuesta WHERE subdomain=%s
            UNION ALL SELECT 'mensajes_cliente', COUNT(*) FROM mensajes_cliente WHERE subdomain=%s
            UNION ALL SELECT 'mensajes_asesor',  COUNT(*) FROM mensajes_asesor  WHERE subdomain=%s
            UNION ALL SELECT 'leads_estado',     COUNT(*) FROM leads_estado     WHERE subdomain=%s
            UNION ALL SELECT 'conversaciones',   COUNT(*) FROM conversaciones   WHERE subdomain=%s
        """, (subdomain,) * 5)
        filas_por_tabla = {r['t']: r['n'] for r in c.fetchall()}

        # --- 3. Un lead concreto ---------------------------------------------
        if not lead_id:
            # El ultimo que respondio un asesor: es el que tiene las dos
            # mitades del dato, entrante y saliente.
            c.execute('SELECT lead_id FROM mensajes_asesor WHERE subdomain = %s '
                      'AND lead_id IS NOT NULL ORDER BY ts DESC LIMIT 1', (subdomain,))
            f = c.fetchone()
            lead_id = f['lead_id'] if f else None

        lead = None
        if lead_id:
            c.execute('SELECT * FROM leads_estado WHERE subdomain=%s AND lead_id=%s',
                      (subdomain, lead_id))
            estado = c.fetchone()
            c.execute('SELECT COUNT(*) AS n, MIN(ts) AS primero, MAX(ts) AS ultimo '
                      'FROM mensajes_cliente WHERE subdomain=%s AND lead_id=%s',
                      (subdomain, lead_id))
            ent = c.fetchone()
            c.execute('SELECT ts, autor_nombre, user_id, origin, message_type '
                      'FROM mensajes_asesor WHERE subdomain=%s AND lead_id=%s '
                      'ORDER BY ts DESC LIMIT 8', (subdomain, lead_id))
            sal = [dict(r) for r in c.fetchall()]
            c.execute('SELECT COUNT(*) AS n FROM tiempos_respuesta '
                      'WHERE subdomain=%s AND lead_id=%s', (subdomain, lead_id))
            tr = c.fetchone()

            e = dict(estado) if estado else {}
            lead = {
                'lead_id': lead_id,
                'link':    f'https://{subdomain}.kommo.com/leads/detail/{lead_id}',
                'leads_estado': {
                    'lead_nombre':         e.get('lead_nombre'),
                    'responsible_user_id': e.get('responsible_user_id'),
                    'responsable':         nombre_asesor(e.get('responsible_user_id')),
                    'asesor_nombre_campo': e.get('asesor_nombre'),
                    'status_id':           e.get('status_id'),
                    'etapa':               nombre_etapa(subdomain, e.get('pipeline_id'),
                                                        e.get('status_id')),
                    'pipeline_id':         e.get('pipeline_id'),
                    'embudo':              nombre_embudo(subdomain, e.get('pipeline_id')),
                    'f_ult_msj_cliente':   local(e.get('f_ult_msj_cliente')),
                    'f_ult_msj_asesor':    local(e.get('f_ult_msj_asesor')),
                } if estado else 'sin fila en leads_estado',
                'mensajes_del_cliente': {
                    'cuantos': ent['n'],
                    'primero': local(ent['primero']),
                    'ultimo':  local(ent['ultimo']),
                },
                'mensajes_del_asesor': [
                    {'hora': local(s['ts']), 'autor': s['autor_nombre'],
                     'user_id': s['user_id'], 'canal': s['origin'],
                     'tipo': s['message_type'],
                     # Misma regla que el dashboard: por nombre, no por user_id.
                     'automatico': s['autor_nombre'] in remitentes_auto_de(subdomain)}
                    for s in sal
                ],
                'respuestas_medidas_por_los_campos': tr['n'],
            }
    finally:
        conn.close()

    return jsonify({
        'subdomain': subdomain,
        'zona_horaria': f'UTC{tz:+d}',
        'llega_de_kommo': llega,
        'guardamos': {
            'tablas': {t: cols for t, cols in guardamos.items()},
            'filas_de_esta_cuenta': filas_por_tabla,
        },
        'un_lead': lead,
    })

@app.route('/health/atencion')
@token_requerido
def health_atencion():
    """Mide sobre que se puede apoyar el ESTADO DE ATENCION en cada cuenta.

    La idea del doble embudo —el de Kommo mide la venta, este mediria la
    atencion— necesita clasificar cada lead en Nuevo / En cola / Atencion Bot /
    Atendido / Finalizado / Abandono. Antes de escribir esa clasificacion hay
    que saber que hay debajo, porque cambia por cuenta:

      - las 2 cuentas suscritas a salientes pueden decir "contesto una PERSONA"
      - las otras 4 solo tienen el campo de fecha del asesor, que es el mismo
        que en Camara China esta viejo y produce 1.556 falsos "sin responder"

    Tres preguntas concretas que el diseño no puede responder solo:

      1. "En cola" (asignado pero sin escribir) solo existe si asignar es un
         paso real. Si Kommo pone responsable a todos al crear el lead, "En
         cola" y "Nuevo" son el mismo grupo con dos nombres. Se mide con
         'sin_atencion_con_responsable'.
      2. "Atencion IA/Bot" no puede ser "el bot le hablo": el Salesbot saluda a
         todos. Se mide cuantos leads tienen MAS de un mensaje del bot, que es
         la unica señal de que hizo algo mas que saludar.
      3. "Finalizado" depende de que status_id signifiquen cierre, y cada
         cuenta arma su embudo. Se devuelve la distribucion para poder mirarla.

    Solo lee."""
    pedido = request.args.get('subdomain', '').strip()
    subs = [pedido] if pedido else list(PULSE_CONFIG)

    salida = {}
    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        for sub in subs:
            if sub not in PULSE_CONFIG:
                continue
            c.execute('''
                SELECT COUNT(*) AS leads,
                       COUNT(responsible_user_id) AS con_responsable,
                       COUNT(f_ult_msj_cliente)   AS con_fecha_cliente,
                       COUNT(f_ult_msj_asesor)    AS con_fecha_asesor,
                       COUNT(asesor_nombre)       AS con_campo_asesor
                  FROM leads_estado WHERE subdomain = %s
            ''', (sub,))
            base = dict(c.fetchone() or {})

            # Lo que aportan los salientes reales. Solo existe donde el evento
            # add_outgoing_message esta suscrito.
            c.execute('''
                SELECT COUNT(DISTINCT lead_id) AS leads_con_saliente,
                       COUNT(DISTINCT lead_id) FILTER (WHERE user_id <> 0) AS leads_con_persona,
                       COUNT(DISTINCT lead_id) FILTER (WHERE autor_nombre = 'Salesbot') AS leads_con_salesbot,
                       COUNT(*) AS mensajes
                  FROM mensajes_asesor WHERE subdomain = %s AND lead_id IS NOT NULL
            ''', (sub,))
            sal = dict(c.fetchone() or {})

            # Pregunta 2: el bot saluda a todos, asi que "le hablo el bot" no
            # separa nada. Lo que separa es que haya hablado MAS de una vez.
            c.execute('''
                SELECT n_bot, COUNT(*) AS leads FROM (
                    SELECT lead_id, COUNT(*) AS n_bot
                      FROM mensajes_asesor
                     WHERE subdomain = %s AND user_id = 0 AND lead_id IS NOT NULL
                     GROUP BY lead_id
                ) t GROUP BY n_bot ORDER BY n_bot LIMIT 8
            ''', (sub,))
            bot_por_lead = [dict(r) for r in c.fetchall()]

            # Pregunta 1: de los leads que NADIE atendio todavia, cuantos ya
            # tienen responsable. Si es casi el 100%, asignar es automatico.
            c.execute('''
                SELECT COUNT(*) AS sin_atencion,
                       COUNT(le.responsible_user_id) AS sin_atencion_con_responsable
                  FROM leads_estado le
                 WHERE le.subdomain = %s
                   AND le.f_ult_msj_cliente IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM mensajes_asesor ma
                        WHERE ma.subdomain = le.subdomain
                          AND ma.lead_id = le.lead_id
                          AND ma.user_id <> 0)
            ''', (sub,))
            cola = dict(c.fetchone() or {})

            # Pregunta 3: que etapas hay. 142/143 son las de Kommo para
            # ganado/perdido; el resto las arma cada cuenta.
            c.execute('''
                SELECT status_id, COUNT(*) AS leads
                  FROM leads_estado WHERE subdomain = %s AND status_id IS NOT NULL
                 GROUP BY status_id ORDER BY leads DESC LIMIT 12
            ''', (sub,))
            etapas = [dict(r) for r in c.fetchall()]

            leads = base.get('leads') or 0
            def pct(n):
                return round(100.0 * n / leads, 1) if leads else None

            salida[sub] = {
                'leads': leads,
                'fuente_disponible': ('mensajes reales' if sal.get('mensajes')
                                      else 'solo campos de fecha'),
                'cobertura': {
                    'con_responsable':   f"{base.get('con_responsable')} ({pct(base.get('con_responsable') or 0)}%)",
                    'con_fecha_cliente': f"{base.get('con_fecha_cliente')} ({pct(base.get('con_fecha_cliente') or 0)}%)",
                    'con_fecha_asesor':  f"{base.get('con_fecha_asesor')} ({pct(base.get('con_fecha_asesor') or 0)}%)",
                    'con_campo_asesor':  base.get('con_campo_asesor'),
                },
                'salientes': sal,
                'p1_en_cola': {
                    **cola,
                    'pct_de_los_no_atendidos': (
                        round(100.0 * (cola.get('sin_atencion_con_responsable') or 0)
                              / cola['sin_atencion'], 1) if cola.get('sin_atencion') else None),
                    'lectura': 'Si es ~100%, asignar es automatico y "En cola" '
                               'no se distingue de "Nuevo".',
                },
                'p2_bot': {
                    'mensajes_de_bot_por_lead': bot_por_lead,
                    'lectura': 'n_bot = 1 suele ser solo el saludo. Los que '
                               'separan de verdad son los de n_bot >= 2.',
                },
                'p3_etapas': etapas,
            }
    finally:
        conn.close()

    return jsonify(salida)

@app.route('/health/remitentes')
@token_requerido
def health_remitentes():
    """Quien manda los mensajes salientes de una cuenta, y como los trata el
    ranking.

    Existe porque la pregunta "¿este nombre lo capturamos?" ya aparecio tres
    veces y los dos endpoints que la respondian —conversacion y salientes—
    escanean el TEXTO CRUDO de la tabla de eventos, sin indice: en la cuenta
    mas grande son 130.000 filas en siete dias y el pedido se cuelga. Este lee
    mensajes_asesor, que ya esta indexada por (subdomain, lead_id, ts).

    Devuelve la clasificacion con las MISMAS reglas que el ranking, no con una
    copia: si manana cambia sin_puesto, esto cambia solo.

    Con &lead_id= se acota a un lead, que es el caso de "abri esta conversacion
    en Kommo y quiero saber que vimos nosotros"."""
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain:
        return jsonify({'error': 'falta ?subdomain='}), 400
    lead_id = request.args.get('lead_id', '').strip()

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        sql = ('SELECT autor_nombre, user_id, author_type, origin, '
               '       COUNT(*) AS mensajes, '
               '       COUNT(DISTINCT lead_id) AS leads, '
               '       MIN(ts) AS desde, MAX(ts) AS hasta '
               '  FROM mensajes_asesor WHERE subdomain = %s')
        params = [subdomain]
        if lead_id:
            sql += ' AND lead_id = %s'
            params.append(lead_id)
        sql += (' GROUP BY autor_nombre, user_id, author_type, origin'
                ' ORDER BY mensajes DESC LIMIT 60')
        c.execute(sql, params)
        filas = [dict(r) for r in c.fetchall()]
    finally:
        conn.close()

    tz = PULSE_CONFIG.get(subdomain, {}).get('tz_offset', 0)
    # Un nombre es CANAL solo si nunca aparecio con un user_id propio. Si al
    # menos una vez llego identificado, es una persona: es la misma regla que
    # usa _calc_por_asesor y por eso se calcula sobre todas las filas antes de
    # clasificar cada una.
    con_user_id = {f['autor_nombre'] for f in filas if f['user_id']}

    def como_lo_ve_el_ranking(f):
        nombre = f['autor_nombre']
        if nombre in REMITENTES_AUTOMATICOS:
            return 'automático · no compite'
        if not f['user_id'] and nombre not in con_user_id:
            return 'sin identificar · persona, pero Kommo no sabe cual'
        return 'compite en el ranking'

    salida = []
    for f in filas:
        nombre = f['autor_nombre']
        propio = nombre_asesor(f['user_id']) if f['user_id'] else None
        salida.append({
            'nombre_en_el_mensaje': nombre,
            'user_id': f['user_id'],
            # Como sale en pantalla: nuestra lista manda sobre el nombre del
            # mensaje cuando el user_id se conoce.
            'nombre_en_el_tablero': nombre_de_remitente(f['user_id'], nombre),
            'en_pulse_config': bool(propio and not propio.startswith('Usuario ')),
            'author_type': f['author_type'],
            'origin': f['origin'],
            'mensajes': f['mensajes'],
            'leads': f['leads'],
            'primero': _ts_to_local(f['desde'], tz).strftime('%d/%m/%Y %H:%M'),
            'ultimo':  _ts_to_local(f['hasta'], tz).strftime('%d/%m/%Y %H:%M'),
            'lo_trata_como': como_lo_ve_el_ranking(f),
        })

    return jsonify({
        'subdomain': subdomain,
        'lead_id': lead_id or 'todos',
        'como_leerlo': ('user_id 0 y nunca identificado = canal o integracion: '
                        'es atencion real de una persona, pero Kommo registra '
                        'el canal y no quien apreto enviar. '
                        'en_pulse_config false = el nombre no esta en nuestra '
                        'lista, asi que se muestra tal cual lo manda Kommo.'),
        'remitentes': salida,
        'sin_capturar': (not salida),
    })

@app.route('/health/conversacion')
@token_requerido
def health_conversacion():
    """Reconstruye UNA conversacion entera —cliente y asesor— para poder
    compararla contra la pantalla de Kommo.

    Existe porque el hallazgo de los mensajes salientes hay que poder
    verificarlo a mano: se abre el lead en Kommo, se abre esto, y las horas
    tienen que coincidir mensaje por mensaje.

    Los entrantes se buscan por la columna lead_id. Los salientes no se pueden:
    se guardan como tipo_evento='desconocido' con lead_id NULL, asi que hay que
    buscarlos dentro del JSON, por el lead que traen adentro (element_id) o por
    el chat_id/talk_id de la conversacion, que se saca de los entrantes.

    Las horas salen en la zona de la cuenta, no en UTC."""
    subdomain = request.args.get('subdomain', '').strip()
    lead_id   = request.args.get('lead_id', '').strip()
    if not subdomain or not lead_id:
        return jsonify({'error': 'faltan subdomain y lead_id'}), 400
    try:
        limite = min(int(request.args.get('limite', 400)), 2000)
    except ValueError:
        limite = 400

    cfg = PULSE_CONFIG.get(subdomain, {})
    tz  = cfg.get('tz_offset', 0)

    def local(ts):
        try:
            return (datetime.utcfromtimestamp(int(ts))
                    + timedelta(hours=tz)).strftime('%d/%m/%Y %H:%M:%S')
        except (TypeError, ValueError, OSError):
            return None

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute(
            "SELECT raw_data FROM eventos WHERE subdomain = %s AND lead_id = %s "
            "AND tipo_evento = 'mensaje' ORDER BY id DESC LIMIT %s",
            (subdomain, lead_id, limite))
        rows_ent = c.fetchall()

        entrantes, chats, talks = [], set(), set()
        for row in rows_ent:
            try:
                data = json.loads(row['raw_data'] or '{}')
            except (ValueError, TypeError):
                continue
            for i in get_batch_indices(data, 'message[add]'):
                p = f'message[add][{i}]'
                if str(data.get(f'{p}[element_id]')) != str(lead_id):
                    continue          # el POST puede traer varios leads
                if data.get(f'{p}[chat_id]'):
                    chats.add(str(data[f'{p}[chat_id]']))
                if data.get(f'{p}[talk_id]'):
                    talks.add(str(data[f'{p}[talk_id]']))
                entrantes.append({
                    'ts':     data.get(f'{p}[created_at]'),
                    'quien':  'CLIENTE',
                    'autor':  data.get(f'{p}[author][name]'),
                    'tipo':   data.get(f'{p}[message_type]'),
                    'canal':  data.get(f'{p}[origin]'),
                    'texto':  (data.get(f'{p}[text]') or '')[:160],
                    'msg_id': data.get(f'{p}[id]'),
                })

        # Un LIKE por el lead y uno por cada chat_id conocido. El LIKE puede
        # traer de mas (el numero puede aparecer en otro campo), asi que
        # despues se confirma parseando el JSON.
        patrones = [f'%{lead_id}%'] + [f'%{cid}%' for cid in list(chats)[:10]]
        c.execute(
            "SELECT raw_data FROM eventos WHERE subdomain = %s "
            "AND tipo_evento = 'desconocido' AND raw_data LIKE ANY(%s) "
            "ORDER BY id DESC LIMIT %s",
            (subdomain, patrones, limite))
        rows_sal = c.fetchall()

        c.execute(
            "SELECT f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, "
            "responsible_user_id, asesor_nombre FROM tiempos_respuesta "
            "WHERE subdomain = %s AND lead_id = %s ORDER BY f_ult_msj_asesor DESC",
            (subdomain, lead_id))
        medidos = c.fetchall()
    finally:
        conn.close()

    salientes, vistos = [], set()
    for row in rows_sal:
        try:
            data = json.loads(row['raw_data'] or '{}')
        except (ValueError, TypeError):
            continue
        for i in get_batch_indices(data, 'outgoing_message[add]'):
            p = f'outgoing_message[add][{i}]'
            elem = str(data.get(f'{p}[element_id]') or '')
            chat = str(data.get(f'{p}[chat_id]') or '')
            talk = str(data.get(f'{p}[talk_id]') or '')
            if elem == str(lead_id):
                via = 'element_id'
            elif chat and chat in chats:
                via = 'chat_id'
            elif talk and talk in talks:
                via = 'talk_id'
            else:
                continue              # el LIKE lo trajo de casualidad
            mid = data.get(f'{p}[id]')
            if mid in vistos:
                continue
            vistos.add(mid)
            uid = str(data.get(f'{p}[author][user_id]') or '0')
            autor = html.unescape((data.get(f'{p}[author][name]') or '').strip())
            # Por NOMBRE y no por user_id = 0, igual que el dashboard. Con el
            # user_id, 'whatsappWZ' y 'WhatsApp Business' salian como bot: son
            # personas contestando por una integracion, y este endpoint existe
            # justamente para verificar lo que muestra la pantalla. Si los dos
            # clasifican distinto, el diagnostico desmiente al producto.
            salientes.append({
                'ts':      data.get(f'{p}[created_at]'),
                'quien':   'AUTOMATICO' if autor in remitentes_auto_de(subdomain) else 'ASESOR',
                'autor':   (nombre_asesor(uid) if uid.isdigit() and uid != '0'
                            else data.get(f'{p}[author][name]')),
                'user_id': uid,
                'tipo':    data.get(f'{p}[message_type]'),
                'canal':   data.get(f'{p}[origin]'),
                'texto':   (data.get(f'{p}[text]') or '')[:160],
                'msg_id':  mid,
                'vinculado_por': via,
            })

    linea = sorted(entrantes + salientes, key=lambda m: int(m['ts'] or 0))
    for m in linea:
        m['hora'] = local(m['ts'])

    # Tiempo de respuesta real: de cada mensaje del cliente sin contestar hasta
    # el primer mensaje de una PERSONA. Los del bot no cuentan como respuesta.
    turnos, esperando = [], None
    for m in linea:
        if m['quien'] == 'CLIENTE':
            if esperando is None:
                esperando = m
        elif m['quien'] == 'ASESOR' and esperando is not None:
            seg = int(m['ts']) - int(esperando['ts'])
            turnos.append({
                'cliente_escribio': esperando['hora'],
                'asesor_respondio': m['hora'],
                'quien_respondio':  m['autor'],
                'espera_seg':       seg,
                'espera_legible':   f'{seg // 3600}h {(seg % 3600) // 60}m {seg % 60}s',
            })
            esperando = None

    return jsonify({
        'lead': f'https://{subdomain}.kommo.com/leads/detail/{lead_id}',
        'zona_horaria': f'UTC{tz:+d} (todas las horas de abajo)',
        'resumen': {
            'mensajes_del_cliente': len(entrantes),
            'mensajes_del_asesor':  sum(1 for m in linea if m['quien'] == 'ASESOR'),
            'mensajes_del_bot':     sum(1 for m in linea if m['quien'] == 'BOT'),
            'chat_ids': sorted(chats), 'talk_ids': sorted(talks),
        },
        'respuestas_reales_del_asesor': turnos,
        'lo_que_mide_el_dashboard_hoy': [{
            'cliente': local(r['f_ult_msj_cliente']),
            'asesor':  local(r['f_ult_msj_asesor']),
            'seg':     r['tiempo_respuesta_seg'],
            'asesor_nombre': r['asesor_nombre'] or nombre_asesor(r['responsible_user_id']),
        } for r in medidos],
        'conversacion': linea,
    })

@app.route('/health/salientes')
@token_requerido
def health_salientes():
    """Los mensajes del ASESOR si llegan, con otro nombre de clave.

    Se dio por hecho durante meses que Kommo no los manda. Lo que pasa es que
    no vienen bajo 'message[add][n]' como los entrantes, sino bajo
    'outgoing_message[add][n]'. _procesar_webhook solo busca el primero, asi
    que todos cayeron en tipo_evento='desconocido' y nunca se miraron: 1.019
    de los ultimos 3.000 desconocidos de tucoytico son mensajes salientes.

    Estan guardados enteros en 'eventos', o sea que se recuperan hacia atras.

    Antes de escribir nada hay que contestar dos cosas, y para eso es este
    endpoint:

      1. QUIEN los manda. Si todos son del bot de bienvenida no sirven para
         medir al asesor. El dato esta en author[type] y author[user_id]:
         user_id = 0 es automatico, distinto de 0 es una persona.
      2. A QUE LEAD pertenecen. Los entrantes traen element_id; los salientes
         no, solo chat_id / talk_id / contact_id. Hay que unirlos por ahi, y
         el puente solo existe si esos ids aparecen tambien en los entrantes,
         que si traen el lead. Eso se mide abajo en 'puente'.

    Solo lee. No escribe ni modifica nada."""
    subdomain = request.args.get('subdomain', '').strip()
    try:
        limite = min(int(request.args.get('limite', 2000)), 8000)
    except ValueError:
        limite = 2000

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        # Se filtra por LIKE y se corta por id DESC: son cientos de miles de
        # filas y no hay indice por contenido, asi que se recorre hacia atras
        # desde lo mas nuevo y se para al llegar al limite.
        q = ("SELECT subdomain, raw_data FROM eventos "
             "WHERE tipo_evento = 'desconocido' AND raw_data LIKE %s")
        params = ['%outgoing_message[add]%']
        if subdomain:
            q += ' AND subdomain = %s'
            params.append(subdomain)
        q += ' ORDER BY id DESC LIMIT %s'
        params.append(limite)
        c.execute(q, params)
        rows = c.fetchall()

        q2 = "SELECT raw_data FROM eventos WHERE tipo_evento = 'mensaje'"
        params2 = []
        if subdomain:
            q2 += ' AND subdomain = %s'
            params2.append(subdomain)
        q2 += ' ORDER BY id DESC LIMIT %s'
        params2.append(limite)
        c.execute(q2, params2)
        rows_ent = c.fetchall()
    finally:
        conn.close()

    por_sub    = defaultdict(int)
    autor_tipo = Counter()
    autor_uid  = Counter()
    tipo_msj   = Counter()
    origen     = Counter()
    claves     = Counter()
    talks_out  = set()
    chats_out  = set()
    talks_hum  = set()
    chats_hum  = set()
    humanos    = []
    n_out = n_humanos = con_lead_directo = 0

    for row in rows:
        try:
            data = json.loads(row['raw_data'] or '{}')
        except (ValueError, TypeError):
            continue
        for i in get_batch_indices(data, 'outgoing_message[add]'):
            p = f'outgoing_message[add][{i}]'
            n_out += 1
            por_sub[row['subdomain']] += 1
            for k in data:
                if k.startswith(p):
                    claves[k[len(p):]] += 1
            uid = str(data.get(f'{p}[author][user_id]'))
            autor_tipo[str(data.get(f'{p}[author][type]'))] += 1
            autor_uid[uid] += 1
            tipo_msj[str(data.get(f'{p}[message_type]'))] += 1
            origen[str(data.get(f'{p}[origin]'))] += 1
            if data.get(f'{p}[element_id]') or data.get(f'{p}[entity_id]'):
                con_lead_directo += 1
            talk, chat = data.get(f'{p}[talk_id]'), data.get(f'{p}[chat_id]')
            if talk:
                talks_out.add(str(talk))
            if chat:
                chats_out.add(str(chat))
            # user_id distinto de 0 = lo escribio una persona, no un bot.
            if uid not in ('0', 'None', ''):
                n_humanos += 1
                if talk:
                    talks_hum.add(str(talk))
                if chat:
                    chats_hum.add(str(chat))
                if len(humanos) < 15:
                    humanos.append({
                        'subdomain':   row['subdomain'],
                        'user_id':     uid,
                        'author_type': data.get(f'{p}[author][type]'),
                        # nombre_asesor hace int(): un user_id no numerico
                        # tumbaria el endpoint entero.
                        'autor':       nombre_asesor(uid) if uid.isdigit() else None,
                        'author_name': data.get(f'{p}[author][name]'),
                        'created_at':  data.get(f'{p}[created_at]'),
                        'talk_id':     talk,
                        'chat_id':     chat,
                        'origin':      data.get(f'{p}[origin]'),
                    })

    # El puente: de los entrantes se saca talk_id/chat_id -> lead, porque esos
    # si traen element_id. Si el id del saliente esta en este mapa, el mensaje
    # del asesor se puede pegar a su lead.
    talk_a_lead, chat_a_lead = {}, {}
    for row in rows_ent:
        try:
            data = json.loads(row['raw_data'] or '{}')
        except (ValueError, TypeError):
            continue
        for i in get_batch_indices(data, 'message[add]'):
            p = f'message[add][{i}]'
            lead = data.get(f'{p}[element_id]')
            if not lead:
                continue
            if data.get(f'{p}[talk_id]'):
                talk_a_lead[str(data[f'{p}[talk_id]'])] = str(lead)
            if data.get(f'{p}[chat_id]'):
                chat_a_lead[str(data[f'{p}[chat_id]'])] = str(lead)

    def pct(a, b):
        return round(100.0 * a / b, 1) if b else None

    return jsonify({
        'muestra': {
            'payloads_salientes': len(rows),
            'mensajes_salientes': n_out,
            'payloads_entrantes_para_el_puente': len(rows_ent),
            'limite': limite,
            'subdomain': subdomain or 'todos',
        },
        'por_subdominio': dict(por_sub),
        'quien_lo_mando': {
            'author_type':          dict(autor_tipo.most_common()),
            'author_user_id':       dict(autor_uid.most_common(15)),
            'de_una_persona':       n_humanos,
            'de_una_persona_pct':   pct(n_humanos, n_out),
            'ejemplos':             humanos,
        },
        'trae_el_lead_directo': {
            'con_element_id': con_lead_directo,
            'nota': 'Si es 0, hay que unirlos por talk_id o chat_id.',
        },
        'puente': {
            'talk_id_conocidos_por_entrantes': len(talk_a_lead),
            'chat_id_conocidos_por_entrantes': len(chat_a_lead),
            'talks_salientes':      len(talks_out),
            'talks_que_cruzan':     len(talks_out & set(talk_a_lead)),
            'talks_que_cruzan_pct': pct(len(talks_out & set(talk_a_lead)), len(talks_out)),
            'chats_salientes':      len(chats_out),
            'chats_que_cruzan':     len(chats_out & set(chat_a_lead)),
            'chats_que_cruzan_pct': pct(len(chats_out & set(chat_a_lead)), len(chats_out)),
            'talks_humanos':        len(talks_hum),
            'talks_humanos_que_cruzan': len(talks_hum & set(talk_a_lead)),
            'chats_humanos':        len(chats_hum),
            'chats_humanos_que_cruzan': len(chats_hum & set(chat_a_lead)),
            'nota': 'Las dos muestras son de momentos distintos, asi que un '
                    'cruce bajo aca no prueba que el puente no exista; subir '
                    '"limite" acerca las dos ventanas.',
        },
        'campos_del_saliente': dict(claves.most_common()),
        'message_type': dict(tipo_msj.most_common()),
        'origin': dict(origen.most_common()),
    })

@app.route('/health/sesgo')
def health_sesgo():
    """Mide cuanto SUBESTIMA Pulse la espera real del cliente.

    'F Ult msj cliente' guarda el ULTIMO mensaje del cliente, no el primero
    sin responder. Si el cliente escribe a las 10:00, 10:05 y 10:10, y el
    asesor contesta 10:12, Pulse mide 2 minutos: el cliente espero 12.

    Kommo solo manda webhook de los mensajes ENTRANTES (verificado: 100% de
    los eventos 'mensaje' tienen type=incoming en las 4 cuentas), asi que la
    hora de la respuesta del asesor no esta en 'eventos'. Se combina entonces:
      - de 'mensajes_cliente':  cada mensaje del cliente con su created_at
      - de 'tiempos_respuesta': f_ult_msj_asesor, la hora de la respuesta

    Lee de 'mensajes_cliente' a proposito, no del JSON crudo de 'eventos': es la
    misma tabla que alimenta la metrica corregida, asi que si el diagnostico y
    el dashboard difieren, el problema esta a la vista y no escondido en dos
    caminos distintos que leen lo mismo de dos formas. Requiere haber corrido
    /backfill-mensajes.

    Es de solo lectura. No devuelve nombres de lead ni texto de mensajes, solo
    ids y duraciones."""
    subdomain = request.args.get('subdomain', '').strip()
    if subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain invalido', 'validos': list(PULSE_CONFIG)}), 400

    cfg = PULSE_CONFIG[subdomain]
    tz_offset    = cfg['tz_offset']
    h_ini, h_fin = cfg['horario']
    dias_lab     = cfg['dias_laborables']

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute(
            'SELECT lead_id, ts FROM mensajes_cliente WHERE subdomain = %s',
            (subdomain,)
        )
        entrantes = [dict(r) for r in c.fetchall()]
        # inicio_pulse es la MISMA expresion que usa /pulse/data para saber
        # desde cuando espera el cliente. Sin traerla, la comparacion se hace
        # contra el metodo viejo y el endpoint reporta como pendiente un sesgo
        # que ya se corrigio.
        # inicio_pulse solo se llena para las filas que el dashboard REALMENTE
        # muestra: las mismas condiciones que /pulse/data. Sin ese recorte se
        # colaban filas con f_ult_msj_cliente viejisimo que el dashboard
        # descarta, y el p90 "de Pulse" daba 780 horas — un numero que nadie ve
        # en pantalla.
        c.execute(
            "SELECT DISTINCT ON (lead_id, f_ult_msj_asesor) "
            "       lead_id, f_ult_msj_asesor AS ts, "
            "       CASE WHEN f_ult_msj_cliente IS NOT NULL "
            "             AND f_ult_msj_asesor > f_ult_msj_cliente "
            "             AND f_ult_msj_asesor - f_ult_msj_cliente <= %s "
            "            THEN LEAST(f_primer_msj_cliente, f_ult_msj_cliente) "
            "       END AS inicio_pulse "
            "FROM tiempos_respuesta "
            "WHERE subdomain = %s AND f_ult_msj_asesor IS NOT NULL "
            "ORDER BY lead_id, f_ult_msj_asesor, "
            "         LEAST(f_primer_msj_cliente, f_ult_msj_cliente) ASC",
            (GAP_REACTIVACION_SEG, subdomain)
        )
        respuestas = [dict(r) for r in c.fetchall()]
    finally:
        conn.close()

    base = {
        'subdomain':            subdomain,
        'mensajes_del_cliente': len(entrantes),
        'respuestas_de_asesor': len(respuestas),
    }

    if not entrantes:
        base['se_pudo_reconstruir'] = False
        base['motivo'] = ('No hay filas en mensajes_cliente para este subdominio. '
                          'Corre /backfill-mensajes?subdomain=' + subdomain + ' primero.')
        return jsonify(base)
    if not respuestas:
        base['se_pudo_reconstruir'] = False
        base['motivo'] = 'No hay respuestas de asesor en tiempos_respuesta para este subdominio.'
        return jsonify(base)

    turnos, descartes = _reconstruir_turnos(
        entrantes, respuestas, tz_offset, h_ini, h_fin, dias_lab
    )
    base['no_medibles'] = {
        'antes_de_nuestro_primer_mensaje': descartes.get('antes_de_nuestro_primer_mensaje', 0),
        'lead_sin_mensajes_capturados':    descartes.get('lead_sin_mensajes_capturados', 0),
        'sin_nadie_esperando':             descartes.get('sin_nadie_esperando', 0),
        'reactivaciones':                  descartes.get('reactivaciones', 0),
        'como_leerlo':
            'Las dos primeras son huecos NUESTROS de captura y no dicen nada de la '
            'cuenta: la espera empezo antes de que estuvieramos escuchando. Solo '
            '"sin_nadie_esperando" es un dato real sobre el CRM — el asesor escribio '
            'sin que hubiera nadie esperando respuesta.',
    }

    if not turnos:
        base['se_pudo_reconstruir'] = False
        base['motivo'] = ('Ninguna respuesta del asesor quedo medible. Mira '
                          '"no_medibles" para saber por que.')
        return jsonify(base)

    reales = [t['real_seg'] for t in turnos]
    # La comparacion solo tiene sentido sobre los turnos que el dashboard
    # muestra. El resto se mide igual —cuentan para la espera real— pero no
    # tienen contra que compararse.
    comparables = [t for t in turnos if t['pulse_seg'] is not None]
    pulses   = [t['pulse_seg'] for t in comparables]
    sesgados = [t for t in comparables if t['sesgo_seg'] > 0]
    peores   = sorted(comparables, key=lambda t: t['sesgo_seg'], reverse=True)[:15]

    base.update({
        'se_pudo_reconstruir': True,
        'turnos_analizados': len(turnos),
        'turnos_visibles_en_pulse': len(comparables),
        'turnos_con_sesgo': len(sesgados),
        'pct_turnos_con_sesgo': round(len(sesgados) / len(comparables) * 100, 1) if comparables else 0,
        'espera_real': {
            'promedio': _fmt_seg(round(sum(reales) / len(reales))),
            'mediana':  _fmt_seg(_pctl(reales, 50)),
            'p90':      _fmt_seg(_pctl(reales, 90)),
        },
        # Ojo: 'espera_real' se calcula sobre TODOS los turnos y esto solo sobre
        # los que el dashboard muestra, asi que las dos medianas no salen del
        # mismo conjunto. Para comparar hay que mirar 'subestimacion', que va
        # turno contra turno.
        'lo_que_mide_pulse_hoy': {
            'promedio': _fmt_seg(round(sum(pulses) / len(pulses))) if pulses else None,
            'mediana':  _fmt_seg(_pctl(pulses, 50)) if pulses else None,
            'p90':      _fmt_seg(_pctl(pulses, 90)) if pulses else None,
        },
        'subestimacion': {
            'promedio': _fmt_seg(round(sum(t['sesgo_seg'] for t in comparables) / len(comparables))) if comparables else None,
            'p90':      _fmt_seg(_pctl([t['sesgo_seg'] for t in comparables], 90)) if comparables else None,
            'peor':     _fmt_seg(max(t['sesgo_seg'] for t in comparables)) if comparables else None,
        },
        'peores_casos': [
            {
                'lead_id':      t['lead_id'],
                'msjs_cliente': t['msjs_cliente'],
                'espera_real':  _fmt_seg(t['real_seg']),
                'pulse_dice':   _fmt_seg(t['pulse_seg']),
            }
            for t in peores
        ],
    })
    return jsonify(base)

# ========================
# PULSE
# ========================
DIAS_LABEL = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']

# Si la brecha entre el ultimo mensaje del cliente y la respuesta del asesor
# supera esto, no es una "respuesta" real sino una reactivacion de un lead
# abandonado (ej. asesor le escribe 600 dias despues) y no debe contar en
# las metricas de tiempo de respuesta.
GAP_REACTIVACION_SEG = 3 * 86400

def _ts_to_local(ts, tz_offset):
    return datetime.utcfromtimestamp(ts) + timedelta(hours=tz_offset)

def _local_date_to_ts(date_str, tz_offset):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return calendar.timegm((dt - timedelta(hours=tz_offset)).timetuple())

def _fmt_horario(h_ini, h_fin):
    def fh(h):
        if h == 0: return '12:00 AM'
        if h < 12: return f'{h}:00 AM'
        if h == 12: return '12:00 PM'
        return f'{h - 12}:00 PM'
    return f'{fh(h_ini)} – {fh(h_fin)}'

def _fmt_seg(seg):
    if seg <= 0: return '< 1 min'
    if seg < 60: return f'{seg}s'
    if seg < 3600:
        m, s = seg // 60, seg % 60
        return f'{m}m {s}s' if s else f'{m}m'
    h, m = seg // 3600, (seg % 3600) // 60
    return f'{h}h {m}m' if m else f'{h}h'

def _next_work_moment(dt, hora_ini, hora_fin, dias_lab):
    dt = dt.replace(second=0, microsecond=0)
    if dt.weekday() in dias_lab and hora_ini <= dt.hour < hora_fin:
        return dt
    if dt.weekday() in dias_lab and dt.hour < hora_ini:
        return dt.replace(hour=hora_ini, minute=0, second=0)
    next_d = (dt + timedelta(days=1)).replace(hour=hora_ini, minute=0, second=0)
    while next_d.weekday() not in dias_lab:
        next_d += timedelta(days=1)
    return next_d

def _work_seconds_between(dt_start, dt_end, hora_ini, hora_fin, dias_lab):
    if dt_start >= dt_end:
        return 0
    total = 0.0
    current = dt_start
    while current < dt_end:
        if current.weekday() in dias_lab:
            ws = current.replace(hour=hora_ini, minute=0, second=0, microsecond=0)
            we = current.replace(hour=hora_fin, minute=0, second=0, microsecond=0)
            ps = max(current, ws)
            pe = min(dt_end, we)
            if ps < pe:
                total += (pe - ps).total_seconds()
        current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(total)

def _calc_tiempo_efectivo(ts_cliente, ts_asesor, tz_offset, hora_ini, hora_fin, dias_lab):
    dt_cliente = _ts_to_local(ts_cliente, tz_offset)
    dt_asesor  = _ts_to_local(ts_asesor,  tz_offset)
    dt_inicio  = _next_work_moment(dt_cliente, hora_ini, hora_fin, dias_lab)
    if dt_asesor <= dt_inicio:
        return 0
    return _work_seconds_between(dt_inicio, dt_asesor, hora_ini, hora_fin, dias_lab)

def _en_horario_laboral(dt, hora_ini, hora_fin, dias_lab):
    return dt.weekday() in dias_lab and hora_ini <= dt.hour < hora_fin

def _pct_enteros(cuentas, total):
    """Porcentajes ENTEROS que suman exactamente 100.

    El decimo de punto porcentual nunca cambio una decision, y ademas promete
    una exactitud que no tenemos: la mediana de alguien con 17 respuestas tiene
    un intervalo de confianza que cubre medio conjunto de sus datos. Decir
    "31.7%" al lado de eso es decorativo, no preciso.

    Pero redondear cada franja por separado NO suma 100: 31.7 + 15.0 + 13.0 +
    38.5 se convierte en 32 + 15 + 13 + 39 = 99, y quien lee las cuatro las
    suma. Se reparte por resto mayor —se trunca todo y las unidades que faltan
    van a los que quedaron mas cerca de subir—, que es lo que garantiza el 100
    exacto. De paso la tira del ranking, que dibuja estos mismos numeros como
    anchos, cierra justo en el borde.
    """
    if not total:
        return [0] * len(cuentas)
    exactos = [c / total * 100 for c in cuentas]
    piso = [int(x) for x in exactos]
    faltan = 100 - sum(piso)
    # De mayor a menor parte decimal: los que estaban mas cerca de subir.
    orden = sorted(range(len(exactos)), key=lambda i: exactos[i] - piso[i], reverse=True)
    for i in orden[:max(0, faltan)]:
        piso[i] += 1
    return piso

def _get_franja_idx(seg, franjas):
    for i, f in enumerate(franjas):
        if f['max_seg'] is None or seg < f['max_seg']:
            return i
    return len(franjas) - 1

def _calc_metricas(registros, franjas, tz_offset):
    """La cifra principal es la MEDIANA, no el promedio.

    Con una distribucion de cola larga el promedio no describe a nadie: en
    corepowerconsulting el promedio da ~3h mientras la mediana da ~17min,
    porque unos pocos casos de 38 horas arrastran a cientos de 5 minutos.
    Ningun cliente espero 3 horas. Se devuelven las tres (mediana, p90 y
    promedio) para poder mostrar el caso tipico y la cola sin que uno tape
    al otro."""
    if not registros:
        return {
            'total': 0,
            'mediana_seg': 0,
            'p90_seg': 0,
            'promedio_seg': 0,
            'maximo_seg': 0,
            'distribucion': [
                {'label': f['label'], 'tag': f['tag'], 'count': 0, 'pct': 0.0, 'color': f['color'], 'leads': []}
                for f in franjas
            ]
        }
    total = len(registros)
    segs = [r['efectivo_seg'] for r in registros]
    mediana_seg  = _pctl(segs, 50)
    p90_seg      = _pctl(segs, 90)
    promedio_seg = round(sum(segs) / total)
    maximo_seg   = max(segs)
    groups = [[] for _ in franjas]
    for r in registros:
        groups[_get_franja_idx(r['efectivo_seg'], franjas)].append(r)
    pcts = _pct_enteros([len(g) for g in groups], total)
    distribucion = []
    for i, f in enumerate(franjas):
        leads = []
        for r in sorted(groups[i], key=lambda x: x['efectivo_seg'], reverse=True)[:50]:
            local_dt = _ts_to_local(r['inicio_espera'], tz_offset)
            leads.append({
                'lead_id': r['lead_id'],
                'nombre': r.get('lead_nombre'),
                'fmt': _fmt_seg(r['efectivo_seg']),
                'fecha': _fmt_fecha_corta(local_dt),
                'asesor': asesor_de_registro(r),
                # Ver _fmt_row: abre la conversacion en la respuesta medida.
                'msg_id': r.get('respuesta_msg_id'),
                'msg_ts': r.get('respuesta_ts'),
            })
        distribucion.append({
            'label': f['label'], 'tag': f['tag'],
            'count': len(groups[i]),
            'pct': pcts[i],
            'color': f['color'],
            'leads': leads,
        })
    return {
        'total': total,
        'mediana_seg': mediana_seg,
        'p90_seg': p90_seg,
        'promedio_seg': promedio_seg,
        'maximo_seg': maximo_seg,
        'distribucion': distribucion,
    }

# Con menos respuestas que esto, el numero de un asesor es ruido: dos
# respuestas rapidas lo ponen primero por encima de alguien con cientos.
MIN_MUESTRA_ASESOR = 10

SIN_ASIGNAR = 'Sin asignar'
OTROS = 'Otros'

# Cuantas personas se dibujan por separado en la tendencia. Core Power llego a
# siete series apiladas y no se podia seguir ninguna: en barras de 20px el ojo
# no distingue siete colores. El resto va junto en "Otros", que sigue sumando
# al total.
MAX_SERIES_TENDENCIA = 5

def _calc_por_asesor(registros, franjas):
    """Ranking de asesores por MEDIANA, no por promedio.

    Los tiempos de respuesta tienen cola larga: una sola respuesta de 8 horas
    arruina el promedio de 40 respuestas de 3 minutos, asi que el promedio
    mide mas los casos raros que el desempeño habitual.

    Quedan fuera del ranking (se muestran igual, al final y sin puesto):
      - los que tienen menos de MIN_MUESTRA_ASESOR respuestas, porque con
        pocos casos el numero es suerte, no desempeño
      - 'Sin asignar', que no es una persona sino los leads sin responsable:
        ponerlo a competir haria que 'nadie' pueda salir primero
      - los BOTS. Se muestran a proposito, con su volumen y su tiempo, porque
        saber cuanto y que tan rapido responde el bot es informacion util. Pero
        no compiten: el Salesbot contesta en 4 segundos y encabezaria el
        ranking todos los meses, empujando a las personas hacia abajo como si
        atendieran peor que una maquina que solo saluda.
      - los CANALES: 'whatsappWZ', 'WhatsApp Business', 'WhatsApp Lite'. Son
        personas contestando desde la app o desde una integracion, o sea
        atencion real, pero Kommo no sabe QUIEN apreto enviar y registra el
        canal. En Core Power son el 54% de los mensajes. No pueden competir en
        un ranking de personas porque no son una persona."""
    # El campo custom "Asesor" manda sobre el responsable cuando esta cargado.
    # En las cuentas que comparten un usuario de Kommo entre varias personas,
    # el responsable no identifica a nadie: el trabajo de tres asesores aparece
    # bajo un solo nombre y el ranking no dice nada. Cuando el campo no viene,
    # se cae al responsable y la cuenta se comporta como siempre.
    grupos = defaultdict(list)
    bots = set()
    # Cuantas de las respuestas de cada uno salieron por un canal compartido y
    # por lo tanto se le atribuyen por ser el RESPONSABLE del lead, no porque
    # se lo haya visto contestar. No cambia el ranking, pero hay que poder
    # decirlo: no es lo mismo un numero medido que uno inferido.
    por_canal = defaultdict(int)
    for r in registros:
        nombre = asesor_de_registro(r) or SIN_ASIGNAR
        grupos[nombre].append(r['efectivo_seg'])
        if r.get('respondio_auto'):
            bots.add(nombre)
        elif not r.get('respondio_user_id') and not r.get('asesor_nombre'):
            por_canal[nombre] += 1

    # Ya no hace falta la categoria 'sin identificar': el nombre de un canal no
    # puede llegar hasta aca. asesor_de_registro solo usa el remitente cuando
    # Kommo lo identifico; si la respuesta salio por WhatsApp Lite o Wazzup, se
    # atribuye al responsable del lead, que si es una persona.
    def sin_puesto(nombre, segs):
        if nombre in bots:
            return 'automático'
        if nombre == SIN_ASIGNAR:
            return 'sin responsable'
        if len(segs) < MIN_MUESTRA_ASESOR:
            return 'pocas respuestas'
        return None

    def reparto(segs):
        """Como se repartieron las respuestas de esta persona entre las franjas.

        La mediana sola engaña: con 15 min de mediana y un p90 de 9 horas, el
        numero grande dice que todo va bien y esconde que una de cada diez
        conversaciones espero casi una jornada. El reparto muestra la cola.

        Ademas se juzga solo: "8% critico" con el total al lado avisa por si
        mismo cuando son una o dos conversaciones. Una mediana calculada sobre
        17 respuestas se ve igual de firme que una sobre 348, y no lo es.

        Van los cuatro tramos siempre, aunque alguno quede en cero: que una
        persona NO tenga criticos es justamente lo que hay que poder ver."""
        cuenta = [0] * len(franjas)
        for s in segs:
            cuenta[_get_franja_idx(s, franjas)] += 1
        # Enteros que suman 100: la tira los dibuja como anchos, asi que si no
        # sumaran 100 quedaria un hueco o se pasaria del borde.
        pcts = _pct_enteros(cuenta, len(segs))
        return [
            {'label': f['label'], 'tag': f['tag'], 'color': f['color'],
             'count': cuenta[i], 'pct': pcts[i]}
            for i, f in enumerate(franjas)
        ]

    resultado = [
        {
            'asesor': nombre,
            'total': len(segs),
            'mediana_seg': _pctl(segs, 50),
            'p90_seg': _pctl(segs, 90),
            'promedio_seg': round(sum(segs) / len(segs)),
            'distribucion': reparto(segs),
            # Cuantas se le atribuyen por ser el responsable del lead y no por
            # haberlo visto contestar: la respuesta salio por un canal
            # compartido y Kommo no dice quien la mando.
            'por_canal': por_canal.get(nombre, 0),
            'sin_puesto': sin_puesto(nombre, segs),
        }
        for nombre, segs in grupos.items()
    ]
    resultado.sort(key=lambda x: (x['sin_puesto'] is not None, x['mediana_seg']))
    return resultado

def _corte_salientes(cursor, subdomain):
    """Desde cuando esta cuenta tiene mensajes salientes propios.

    Es la frontera entre los dos metodos de medicion. Antes de esta hora la
    respuesta del asesor solo se sabe por el campo de fecha de Kommo; desde
    aca se sabe por el mensaje, con el segundo exacto y con quien lo mando.

    Se calcula por cuenta y no con una fecha fija porque cada una se suscribio
    en un momento distinto: tucoytico y corepowerconsulting el 18/08/2026, las
    otras cuatro el 24/08/2026.

    None = esa cuenta no tiene salientes y todo se mide como antes."""
    cursor.execute('SELECT MIN(ts) AS corte FROM mensajes_asesor WHERE subdomain = %s',
                   (subdomain,))
    fila = cursor.fetchone()
    return fila['corte'] if fila else None

# Turnos armados por cuenta, para no rearmarlos en cada carga. Armarlos es la
# parte cara del tablero —cargar todos los mensajes desde el corte y
# recorrerlos en Python, 1 s medido en Camara China— y NO depende de ningun
# filtro: periodo, responsable, embudo y horario se aplican despues, sobre los
# turnos ya hechos. Asi que se guardan y se reusan.
#
# La clave es el ultimo ts de cada tabla de mensajes: dos consultas de indice
# que cuestan milisegundos. Si entro un mensaje nuevo, la clave cambia y se
# rearma; si no, se reusa tal cual. No hay vencimiento por tiempo porque no
# hace falta: el dato es exacto o se rehace. Lo que cambia de filtro —el caso
# de "pongo un periodo y dice cargando"— entra de lleno.
#
# Los turnos se devuelven como estan y quien los usa no los modifica: los
# registros se construyen aparte, en _registros_de_mensajes.
_turnos_cache = {}     # subdomain -> (clave, turnos)

def _turnos_cacheados(cursor, subdomain, corte):
    cursor.execute('SELECT MAX(ts) AS m FROM mensajes_cliente WHERE subdomain = %s', (subdomain,))
    ult_cli = (cursor.fetchone() or {}).get('m')
    cursor.execute('SELECT MAX(ts) AS m FROM mensajes_asesor WHERE subdomain = %s', (subdomain,))
    ult_asr = (cursor.fetchone() or {}).get('m')
    clave = (corte, ult_cli, ult_asr)
    v = _turnos_cache.get(subdomain)
    if v and v[0] == clave:
        return v[1]

    # Los entrantes se leen desde antes del corte a proposito: un cliente pudo
    # escribir el dia anterior y recibir respuesta despues del corte. Si se
    # leyeran solo desde el corte, ese turno se mediria desde un mensaje que no
    # es el primero sin contestar.
    cursor.execute(
        'SELECT lead_id, ts FROM mensajes_cliente WHERE subdomain = %s AND ts >= %s',
        (subdomain, corte - GAP_REACTIVACION_SEG))
    entrantes = [(r['lead_id'], r['ts']) for r in cursor.fetchall()]

    cursor.execute(
        'SELECT lead_id, ts, autor_nombre, user_id, msg_id FROM mensajes_asesor '
        'WHERE subdomain = %s AND ts >= %s AND lead_id IS NOT NULL',
        (subdomain, corte))
    salientes = [(r['lead_id'], r['ts'], r['autor_nombre'], r['user_id'], r['msg_id'])
                 for r in cursor.fetchall()]

    turnos = []
    if salientes:
        turnos = _turnos_de_mensajes(entrantes, salientes, remitentes_auto_de(subdomain))
        # La frontera se decide por CUANDO SE RESPONDIO, no por cuando escribio
        # el cliente. Asi cada respuesta cae en un metodo y en uno solo: la
        # consulta vieja se corta con f_ult_msj_asesor < corte. Si se partiera
        # por el inicio, una respuesta del borde quedaria contada dos veces.
        turnos = [t for t in turnos
                  if t['fin'] >= corte and t['seg'] <= GAP_REACTIVACION_SEG]
    _turnos_cache[subdomain] = (clave, turnos)
    return turnos

def _registros_de_mensajes(cursor, subdomain, corte, tz_offset, h_ini, h_fin, dias_lab,
                           fuera_horario, desde_ts, hasta_ts, asesor_ids, asesores_nombre,
                           embudos_sel=(), etapas_sel=()):
    """Arma los registros del dashboard desde los MENSAJES, no desde los campos.

    Devuelve exactamente la misma forma que la consulta vieja, para que todo lo
    que sigue —metricas, ranking, distribucion, tendencia, extremos— no tenga
    que saber de donde salio cada fila.

    Dos diferencias de fondo con el metodo viejo:
      - mide desde el PRIMER mensaje sin contestar y cuenta TODOS los turnos.
        Los campos guardan un solo valor y cada respuesta pisa la anterior: en
        el lead 47412902 quedaba 1 respuesta donde hubo 7.
      - sabe quien contesto de verdad, incluido si fue un bot.

    Los entrantes se leen desde antes del corte a proposito: un cliente pudo
    escribir el dia anterior y recibir respuesta despues del corte. Si se
    leyeran solo desde el corte, ese turno se mediria desde un mensaje que no
    es el primero sin contestar."""
    if not corte:
        return []

    # Los turnos ya armados, o rearmados si entro un mensaje nuevo. Ver
    # _turnos_cacheados: es la parte cara y no depende de ningun filtro.
    turnos = _turnos_cacheados(cursor, subdomain, corte)
    if not turnos:
        return []

    # Responsable, nombre del lead y campo custom 'Asesor'. Sale de
    # leads_estado, que ya tiene una fila por lead.
    leads = sorted({t['lead_id'] for t in turnos})
    sql_emb, par_emb = _filtro_embudo(embudos_sel, etapas_sel)
    cursor.execute(
        'SELECT lead_id, responsible_user_id, lead_nombre, asesor_nombre '
        'FROM leads_estado WHERE subdomain = %s AND lead_id = ANY(%s)' + sql_emb,
        tuple([subdomain, leads] + par_emb))
    meta = {r['lead_id']: r for r in cursor.fetchall()}

    registros = []
    filtra_embudo = bool(embudos_sel or etapas_sel)
    for t in turnos:
        m = meta.get(t['lead_id'])
        # Con filtro de embudo, la consulta de arriba ya devolvio SOLO los
        # leads que pasan. Un lead que no esta en 'meta' quedo fuera y no
        # puede colarse con los datos vacios.
        if m is None:
            if filtra_embudo:
                continue
            m = {}

        if desde_ts is not None and t['inicio'] < desde_ts:
            continue
        if hasta_ts is not None and t['inicio'] > hasta_ts:
            continue
        if asesor_ids and m.get('responsible_user_id') not in asesor_ids:
            continue
        if asesores_nombre and (m.get('asesor_nombre') or '') not in asesores_nombre:
            continue

        dt_cliente = _ts_to_local(t['inicio'], tz_offset)
        if fuera_horario:
            if _en_horario_laboral(dt_cliente, h_ini, h_fin, dias_lab):
                continue
            efectivo = t['seg']
        else:
            efectivo = _calc_tiempo_efectivo(t['inicio'], t['fin'],
                                             tz_offset, h_ini, h_fin, dias_lab)

        registros.append({
            'lead_id':             t['lead_id'],
            'lead_nombre':         (m.get('lead_nombre') or '').strip() or None,
            'inicio_espera':       t['inicio'],
            'efectivo_seg':        efectivo,
            'responsible_user_id': m.get('responsible_user_id'),
            'asesor_nombre':       (m.get('asesor_nombre') or '').strip() or None,
            # Quien mando el mensaje de verdad, y si fue un bot. Los lee
            # asesor_de_registro para armar el ranking.
            'respondio_nombre':    t['autor'],
            'respondio_auto':      t['auto'],
            # user_id 0 sin ser bot = contesto una PERSONA pero por fuera de
            # Kommo, desde la app o una integracion como Wazzup. Kommo no sabe
            # quien apreto enviar y registra el canal. Es atencion real, pero
            # no se puede atribuir a nadie, y el ranking tiene que decirlo.
            'respondio_user_id':   t['user_id'],
            # Con que mensaje se contesto y cuando. Es lo que deja abrir la
            # conversacion parada en esa respuesta en vez de al final.
            # Solo existe en el metodo nuevo: las filas anteriores al corte no
            # tienen mensaje propio y el link se degrada al lead.
            'respuesta_msg_id':    t.get('msg_id'),
            'respuesta_ts':        t['fin'],
        })
    return registros

# Cuantas respuestas se promedian por lead en la lista de mas lentas/rapidas.
TOP_ULTIMAS_RESPUESTAS = 5

# Cuantas hacen falta como MINIMO para que un lead entre en esa lista.
#
# Con una sola respuesta el numero es una anecdota: basta que un cliente
# escriba un viernes a la tarde para que ese lead encabece la lista de los mas
# lentos sin que eso diga nada del equipo. Con tres ya se ve un patron.
#
# El costo es que la lista se acorta, y en las cuentas con poca conversacion
# puede quedar vacia. Es preferible una lista corta y confiable a una larga
# donde los primeros puestos son casualidad.
MIN_RESPUESTAS_EXTREMOS = 3

def _fmt_row(r, tz_offset):
    local_dt = _ts_to_local(r['inicio_espera'], tz_offset)
    return {
        'lead_id': r['lead_id'],
        'nombre': r.get('lead_nombre'),
        'seg': r['efectivo_seg'],
        'fmt': _fmt_seg(r['efectivo_seg']),
        'fecha': _fmt_fecha_corta(local_dt),
        'asesor': asesor_de_registro(r),
        # Sobre cuantas respuestas se promedio: sin esto, un lead con una sola
        # respuesta y otro con cinco se leen igual.
        'respuestas': r.get('respuestas', 1),
        # Para abrir la conversacion EN la respuesta que cerro la espera, no al
        # final del chat. Van los dos: Kommo necesita el timestamp para ubicar
        # el tramo y el id para marcar el mensaje. Probado que el segundo
        # redondeado alcanza, no hacen falta los milisegundos.
        'msg_id': r.get('respuesta_msg_id'),
        'msg_ts': r.get('respuesta_ts'),
    }

def _hoy_rango_ts(tz_offset):
    hoy_local = datetime.utcnow() + timedelta(hours=tz_offset)
    inicio = _local_date_to_ts(hoy_local.strftime('%Y-%m-%d'), tz_offset)
    return inicio, inicio + 86400

def _fmt_fecha_corta(local_dt):
    """'29/07 11:24', o '20/09/25 16:24' si no es de este anio.

    Sin el anio, un lead de septiembre del anio pasado se ve identico a uno
    de este mes y no hay forma de distinguir lo urgente de lo muerto."""
    if local_dt.year == datetime.utcnow().year:
        return local_dt.strftime('%d/%m %H:%M')
    return local_dt.strftime('%d/%m/%y %H:%M')

# Nombre de respaldo para los leads que Kommo nunca bautizo. Se toma el mas
# reciente por si el contacto se renombro. Una sola definicion porque la usan
# la lista principal y las dos tarjetas del snapshot: cuando estaba solo en la
# principal, "Sin responder" seguia mostrando "Sin nombre" con el dato al lado.
SQL_NOMBRES_CLIENTE = '''
    SELECT DISTINCT ON (lead_id) lead_id, cliente_nombre
    FROM mensajes_cliente
    WHERE subdomain = %s
      AND lead_id = ANY(%s)
      AND cliente_nombre IS NOT NULL
    ORDER BY lead_id, ts DESC
'''

def _completar_nombres(subdomain, *listas, conn=None):
    """Rellena el 'nombre' vacio de varias listas de leads, en UNA consulta.

    Recibe todas las listas juntas a proposito: son dos tarjetas que se dibujan
    en la misma pantalla y pedirlas por separado seria abrir dos conexiones
    contra Supabase para lo mismo. Modifica las listas en el lugar."""
    faltan = [f['lead_id'] for lst in listas for f in lst
              if not (f.get('nombre') or '').strip()]
    if not faltan:
        return
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute(SQL_NOMBRES_CLIENTE, (subdomain, list(set(faltan))))
        nombres = {r['lead_id']: r['cliente_nombre'] for r in c.fetchall()}
    finally:
        if propia:
            conn.close()
    for lst in listas:
        for f in lst:
            if not (f.get('nombre') or '').strip():
                f['nombre'] = nombres.get(f['lead_id'])

def _fmt_lead_estado(row, tz_offset, campo_fecha):
    local_dt = _ts_to_local(row[campo_fecha], tz_offset)
    return {
        'lead_id': row['lead_id'],
        'nombre':  row.get('lead_nombre'),
        # Mismo criterio que el ranking: si mostrara el responsable mientras el
        # ranking muestra al asesor, el mismo lead aparece con dos nombres
        # distintos en la misma pantalla.
        'asesor':  asesor_de_registro(row),
        'fecha':   _fmt_fecha_corta(local_dt),
    }

# Constantes reservadas de Kommo, iguales en cualquier cuenta/pipeline:
# 142 = lead Ganado, 143 = lead Perdido. Un status_id NULL (lead sin
# eventos de status capturados aun) se trata como abierto por defecto.
STATUS_CERRADOS = (142, 143)
FILTRO_SOLO_ABIERTAS = 'AND (status_id IS NULL OR status_id NOT IN (142, 143))'

def _filtro_embudo(embudos, etapas):
    """Fragmento SQL para acotar por embudo y por etapa. Se aplica sobre
    leads_estado, que es donde vive pipeline_id.

    Dos listas porque el selector es anidado: se puede tildar un embudo entero
    ('VENTAS') o solo algunas de sus etapas. Van con OR, asi que tildar el
    embudo y ademas una etapa suya no achica el resultado.

    Las etapas viajan como "pipeline:status" y no como status suelto. Kommo
    reserva 142 y 143 para ganado y perdido, asi que esos dos ids se repiten en
    TODOS los embudos con nombres distintos —en Autonica el 142 es "CITAS
    PROGRAMADAS" y en Camara China "ENTREGADO / FINALIZADO"—. Filtrar por 142 a
    secas mezclaria etapas de embudos que no tienen nada que ver."""
    if not embudos and not etapas:
        return '', []
    partes, params = [], []
    if embudos:
        partes.append('pipeline_id = ANY(%s)')
        params.append(embudos)
    if etapas:
        partes.append("(pipeline_id::text || ':' || status_id::text) = ANY(%s)")
        params.append(etapas)
    return ' AND (' + ' OR '.join(partes) + ')', params

def _embudos_con_leads(cursor, subdomain):
    """Los embudos y etapas que REALMENTE tienen leads en esta cuenta, con su
    conteo. Se lista lo que hay y no los 24 embudos que existen en Camara
    China: un selector con veinte opciones vacias no se puede usar."""
    cursor.execute(
        'SELECT pipeline_id, status_id, COUNT(*) AS leads FROM leads_estado '
        'WHERE subdomain = %s AND pipeline_id IS NOT NULL '
        'GROUP BY pipeline_id, status_id', (subdomain,))
    por_embudo = defaultdict(lambda: {'leads': 0, 'etapas': []})
    for r in cursor.fetchall():
        e = por_embudo[r['pipeline_id']]
        e['leads'] += r['leads']
        if r['status_id'] is not None:
            e['etapas'].append({
                'id':     r['status_id'],
                'clave':  f"{r['pipeline_id']}:{r['status_id']}",
                'nombre': nombre_etapa(subdomain, r['pipeline_id'], r['status_id']),
                'leads':  r['leads'],
            })
    salida = [
        {
            'id':     pid,
            'nombre': nombre_embudo(subdomain, pid),
            'leads':  v['leads'],
            'etapas': sorted(v['etapas'], key=lambda x: -x['leads']),
        }
        for pid, v in por_embudo.items()
    ]
    salida.sort(key=lambda x: -x['leads'])
    return salida

# Un dia con menos de esta fraccion del volumen habitual de la cuenta no se
# capturo entero. 0.30 y no 0: el apagon del 31/08 dejo unos minutos de datos
# en cada punta, asi que mirar "dias con CERO eventos" no lo detecta —que es
# justamente lo que pasaba con el piso de captura, que trabaja por dia—.
FRACCION_DIA_COMPLETO = 0.30

def _dias_parciales(subdomain, conn=None):
    """Dias en los que capturamos MUY por debajo de lo habitual.

    Un numero historico calculado sobre un dia asi miente: parece que nadie
    contesto cuando lo que pasa es que no estabamos escuchando. Se compara
    contra la mediana de mensajes por dia de la propia cuenta, no contra un
    umbral fijo, porque el volumen va de 600 mensajes diarios en Camara China
    a 20 en Core Power.

    Se mide sobre mensajes_asesor, que es chica y esta indexada por
    (subdomain, ts); recorrer 'eventos' costaria un segundo."""
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT TO_CHAR(TO_TIMESTAMP(ts), 'YYYY-MM-DD'), COUNT(*) "
            'FROM mensajes_asesor WHERE subdomain = %s GROUP BY 1',
            (subdomain,))
        por_dia = dict(c.fetchall())
    finally:
        if propia:
            conn.close()
    if len(por_dia) < 3:
        return set()
    cuentas = sorted(por_dia.values())
    mediana = cuentas[len(cuentas) // 2]
    piso = mediana * FRACCION_DIA_COMPLETO
    return {dia for dia, n in por_dia.items() if n < piso}

_DIAS_PARCIALES_TTL_SEG = 3600
_dias_parciales_cache = {}

def dias_parciales(subdomain, conn=None):
    """_dias_parciales con cache de una hora: cambia una vez por dia."""
    ahora = time.monotonic()
    v = _dias_parciales_cache.get(subdomain)
    if v and v[1] > ahora:
        return v[0]
    valor = _dias_parciales(subdomain, conn)
    _dias_parciales_cache[subdomain] = (valor, ahora + _DIAS_PARCIALES_TTL_SEG)
    return valor

def _leads_no_respondidos(subdomain, tz_offset, responsible_user_ids=None,
                          solo_abiertas=False, desde_ts=None,
                          sql_embudo='', params_embudo=(), corte=None, conn=None,
                          hasta_ts=None):
    """Leads cuyo último mensaje es del cliente: nadie contestó después.

    Se calcula con los MENSAJES y desde el corte, no con los campos de fecha
    de Kommo. Los campos tenían tres problemas, y los tres inflaban el número:

      - 'f_ult_msj_asesor IS NULL' no quiere decir que nadie contestó, quiere
        decir que el campo está vacío. De ahí salían los 1.556 falsos
        positivos de Cámara China.
      - vienen redondeados al minuto: cliente 10:30:50 y asesor 10:30:20 dan
        los dos '10:30' y la comparación falla.
      - se desactualizan. En el lead 23798351 el campo decía 11:20 y los
        mensajes reales del asesor eran de 16:52, 17:04 y 17:08.

    El precio es que el número solo cubre desde el corte: antes de esa fecha
    Kommo no nos mandaba los salientes y no hay con qué reemplazar el campo.
    Se prefiere un número más chico y cierto a uno grande que nadie puede
    usar; la fecha se muestra en la tarjeta.

    Cuenta como respuesta CUALQUIER saliente, incluido el del bot: la tarjeta
    dice "sin responder", y el bot respondió. Que ese saludo no sea atención
    de verdad es otra pregunta, y se mide aparte.

    Sin corte —cuenta sin salientes capturados— se cae al método viejo."""
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        if corte:
            query = '''
                SELECT le.lead_id, le.responsible_user_id, le.lead_nombre,
                       le.asesor_nombre, cli.ultimo AS f_ult_msj_cliente
                  FROM leads_estado le
                  JOIN (SELECT lead_id, MAX(ts) AS ultimo FROM mensajes_cliente
                         WHERE subdomain = %s AND ts >= %s AND (%s IS NULL OR ts <= %s)
                         GROUP BY lead_id) cli
                    ON cli.lead_id = le.lead_id
             LEFT JOIN (SELECT lead_id, MAX(ts) AS ultimo FROM mensajes_asesor
                         WHERE subdomain = %s AND lead_id IS NOT NULL
                           AND (%s IS NULL OR ts <= %s)
                         GROUP BY lead_id) asr
                    ON asr.lead_id = le.lead_id
                 WHERE le.subdomain = %s
                   AND (asr.ultimo IS NULL OR cli.ultimo > asr.ultimo)
            '''
            # hasta_ts reconstruye el estado en un momento PASADO: quienes
            # estaban esperando a esa hora. No hace falta guardar ninguna foto
            # —tenemos cada mensaje con su timestamp de los dos lados—, alcanza
            # con no mirar lo que llego despues. Con hasta_ts=None es el ahora.
            params = [subdomain, corte, hasta_ts, hasta_ts,
                      subdomain, hasta_ts, hasta_ts, subdomain]
        else:
            query = '''
                SELECT lead_id, responsible_user_id, f_ult_msj_cliente, lead_nombre, asesor_nombre
                FROM leads_estado
                WHERE subdomain = %s
                  AND f_ult_msj_cliente IS NOT NULL
                  AND (f_ult_msj_asesor IS NULL OR f_ult_msj_cliente > f_ult_msj_asesor)
            '''
            params = [subdomain]
            if desde_ts is not None:
                query += ' AND f_ult_msj_cliente >= %s'
                params.append(desde_ts)
        if responsible_user_ids:
            query += ' AND responsible_user_id = ANY(%s)'
            params.append(list(responsible_user_ids))
        if solo_abiertas:
            query += ' ' + FILTRO_SOLO_ABIERTAS
        if sql_embudo:
            query += sql_embudo
            params.extend(params_embudo)
        query += ' ORDER BY f_ult_msj_cliente DESC'
        c.execute(query, params)
        rows = c.fetchall()
    finally:
        if propia:
            conn.close()

    # Cuanto lleva esperando cada uno, en segundos EFECTIVOS (horario laboral
    # de la cuenta), para que el tablero le ponga la misma franja que usa el
    # analisis del periodo. Pedido en la reunion del 31/08: la lista tenia 229
    # leads ordenados por fecha y no habia forma de saber a cual atender
    # primero. Con la espera se ordena y se pinta; sin ella es una lista.
    #
    # Efectivo y no real a proposito: un cliente que escribio a las 19:00 y
    # son las 8:05 lleva 13 horas reales pero 5 minutos laborales, y la franja
    # "Critico" sobre las 13 horas diria que el equipo fallo cuando estaba
    # cerrado. Es el mismo criterio del numero grande.
    cfg = PULSE_CONFIG.get(subdomain, {})
    h_ini, h_fin = cfg.get('horario', (0, 24))
    dias_lab = cfg.get('dias_laborables', [0, 1, 2, 3, 4, 5, 6])
    ahora = int(time.time())
    salida = []
    for r in rows:
        fila = _fmt_lead_estado(r, tz_offset, 'f_ult_msj_cliente')
        fila['espera_seg'] = _calc_tiempo_efectivo(
            r['f_ult_msj_cliente'], ahora, tz_offset, h_ini, h_fin, dias_lab)
        salida.append(fila)
    return salida

def _dia_valido(subdomain, tz_offset, fecha, corte, conn):
    """(inicio, fin_del_dia) de esa fecha, o None si no se puede reconstruir.

    Se niega cuando el numero no seria honesto:
      - antes del corte no teniamos los salientes, asi que todo pareceria sin
        responder;
      - un dia con captura parcial —el apagon del 31/08— da lo mismo;
      - una fecha futura no existe.
    Es preferible no mostrar un numero a mostrar uno que miente."""
    if not fecha or not corte:
        return None
    try:
        inicio = _local_date_to_ts(fecha, tz_offset)
    except (ValueError, TypeError):
        return None
    # HOY no cuenta como dia reconstruible: todavia no termino. Para el ahora
    # esta el camino en vivo, y como referencia del comparativo seria injusto
    # —medio dia contra un dia entero—.
    hoy_inicio, _ = _hoy_rango_ts(tz_offset)
    if inicio < corte or inicio >= hoy_inicio:
        return None
    if fecha in dias_parciales(subdomain, conn):
        return None
    return inicio, inicio + 86400

def estado_de_dia(subdomain, tz_offset, h_ini, h_fin, fecha, corte, conn,
                  asesor_ids=None, solo_abiertas=False,
                  sql_embudo='', params_embudo=(), hasta_ts=None):
    """El estado del snapshot en un dia PASADO: las mismas dos listas.

    No hace falta ninguna foto guardada: tenemos cada mensaje entrante y
    saliente con su timestamp, asi que el estado de cualquier momento se
    reconstruye mirando solo lo que habia llegado hasta entonces. Esto se
    creyo imposible por un rato y no lo es.

    'hasta_ts' corta el dia a una hora. Se usa cuando el otro lado de la
    comparacion es HOY, que va por la mitad: comparar medio dia contra un dia
    entero infla la diferencia. Entre dos dias pasados no se corta nada,
    porque los dos terminaron.

    Devuelve las listas completas y no solo los conteos: son las mismas que
    dibujan los paneles, asi que elegir una fecha cambia todo el bloque sin
    codigo nuevo."""
    rango = _dia_valido(subdomain, tz_offset, fecha, corte, conn)
    if rango is None:
        return None
    inicio, fin = rango
    tope = min(hasta_ts, fin) if hasta_ts else fin

    sin_responder = _leads_no_respondidos(
        subdomain, tz_offset, asesor_ids, solo_abiertas, corte,
        sql_embudo, params_embudo, corte, conn=conn, hasta_ts=tope)
    trabajados, solo_bot = _leads_trabajados_hoy(
        subdomain, tz_offset, h_ini, h_fin, asesor_ids, solo_abiertas,
        sql_embudo, params_embudo, corte, conn=conn, rango=(inicio, tope))
    _completar_nombres(subdomain, sin_responder, trabajados, solo_bot, conn=conn)
    return {
        'fecha':              fecha,
        'no_respondidos':     sin_responder,
        'trabajados_hoy':     trabajados,
        'atendidos_solo_bot': solo_bot,
    }

def _hora_del_dia(tz_offset):
    """Cuantos segundos lleva el dia de hoy, en la zona de la cuenta."""
    hoy_inicio, _ = _hoy_rango_ts(tz_offset)
    return int(time.time()) - hoy_inicio

def _ayer_de(tz_offset, fecha=None):
    """El dia anterior a 'fecha', o a hoy si no se pasa ninguna."""
    if fecha:
        try:
            base = datetime.strptime(fecha, '%Y-%m-%d')
        except (ValueError, TypeError):
            base = datetime.utcnow() + timedelta(hours=tz_offset)
    else:
        base = datetime.utcnow() + timedelta(hours=tz_offset)
    return (base - timedelta(days=1)).strftime('%Y-%m-%d')

def _bloque_ahora(subdomain, tz_offset, h_ini, h_fin, corte, piso, conn,
                  asesor_ids=None, solo_abiertas=False, sql_embudo='', params_embudo=()):
    """Todo el bloque de operacion: las dos listas y su comparativo.

    Por defecto es el AHORA. Con ?dia=YYYY-MM-DD pasa a ser el estado de ese
    dia —los dos numeros, las listas y la tabla por asesor, todo—, porque son
    las mismas listas. Con ?comparar=YYYY-MM-DD se elige contra que dia se
    compara; por defecto, el dia anterior al que se este mirando.

    El corte por hora solo aplica cuando la base es hoy: hoy va por la mitad
    y compararlo contra un dia entero infla la diferencia. Entre dos dias
    pasados se toman enteros los dos."""
    dia = (request.args.get('dia') or '').strip()
    base_es_hoy = True
    if dia:
        est = estado_de_dia(subdomain, tz_offset, h_ini, h_fin, dia, corte, conn,
                            asesor_ids, solo_abiertas, sql_embudo, params_embudo)
        if est is not None:
            base_es_hoy = False
            no_respondidos = est['no_respondidos']
            trabajados     = est['trabajados_hoy']
            solo_bot       = est['atendidos_solo_bot']
    if base_es_hoy:
        # La fecha pedida no sirve (antes del corte, dia parcial, futura): se
        # cae al ahora y se dice cual se uso, para que la pantalla no muestre
        # una fecha elegida y numeros de otra.
        dia = None
        no_respondidos = _leads_no_respondidos(subdomain, tz_offset, asesor_ids, solo_abiertas,
                                               piso, sql_embudo, params_embudo, corte, conn=conn)
        trabajados, solo_bot = _leads_trabajados_hoy(subdomain, tz_offset, h_ini, h_fin,
                                                     asesor_ids, solo_abiertas,
                                                     sql_embudo, params_embudo, corte, conn=conn)
        _completar_nombres(subdomain, no_respondidos, trabajados, solo_bot, conn=conn)

    contra = (request.args.get('comparar') or '').strip() or _ayer_de(tz_offset, dia)
    comparado = comparar_snapshot(subdomain, tz_offset, h_ini, h_fin, contra,
                                  corte, conn, asesor_ids, solo_abiertas,
                                  sql_embudo, params_embudo, base_es_hoy=base_es_hoy)
    return {
        'no_respondidos':     no_respondidos,
        'trabajados_hoy':     trabajados,
        'atendidos_solo_bot': solo_bot,
        'comparado':          comparado,
        # Que dia se esta mirando de verdad. None = hoy, en vivo.
        'dia': dia,
    }

def comparar_snapshot(subdomain, tz_offset, h_ini, h_fin, fecha, corte, conn,
                      asesor_ids=None, solo_abiertas=False,
                      sql_embudo='', params_embudo=(), base_es_hoy=True):
    """Los dos numeros del snapshot en un dia pasado, para el comparativo.

    Con base_es_hoy se corta a la MISMA hora que lleva hoy; entre dos dias
    pasados se toman los dias enteros. Ver estado_de_dia."""
    inicio = _dia_valido(subdomain, tz_offset, fecha, corte, conn)
    if inicio is None:
        return None
    hasta = inicio[0] + _hora_del_dia(tz_offset) if base_es_hoy else None
    est = estado_de_dia(subdomain, tz_offset, h_ini, h_fin, fecha, corte, conn,
                        asesor_ids, solo_abiertas, sql_embudo, params_embudo,
                        hasta_ts=hasta)
    if est is None:
        return None
    return {
        'fecha':          fecha,
        'no_respondidos': len(est['no_respondidos']),
        'trabajados_hoy': len(est['trabajados_hoy']) + len(est['atendidos_solo_bot']),
    }

def _leads_trabajados_hoy(subdomain, tz_offset, h_ini, h_fin, responsible_user_ids=None,
                          solo_abiertas=False, sql_embudo='', params_embudo=(),
                          corte=None, conn=None, rango=None):
    """Leads que recibieron respuesta hoy, SEPARADOS en dos listas: los que
    atendio una persona y los que solo contesto el bot.

    Devuelve (por_persona, solo_bot).

    Por que separarlos: el bot saluda a todos los leads nuevos, y ese saludo
    deja el ultimo mensaje del lado nuestro. O sea que el lead deja de figurar
    en "Sin responder" y pasa a figurar en "Atendidos hoy" sin que ninguna
    persona lo haya mirado. Es el punto ciego del tablero: los que PARECEN
    atendidos y no lo estan.

    Se calcula desde 'mensajes_asesor' y no desde el campo f_ult_msj_asesor,
    que es lo que se hacia antes. El campo no dice quien escribio: se actualiza
    igual si contesto el bot, asi que mezclaba los dos en un solo numero.

    Sin 'corte' (cuenta sin salientes capturados) se cae al metodo viejo y todo
    queda en la lista de personas, que es como se comportaba hasta ahora."""
    # 'rango' permite pedir otro dia —el comparativo contra ayer— con la misma
    # consulta: son mensajes con timestamp, no una foto guardada.
    inicio, fin = rango if rango else _hoy_rango_ts(tz_offset)
    autos = list(remitentes_auto_de(subdomain))
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        if corte:
            # ultimo_de_persona es NULL cuando lo unico que llego hoy fue del
            # bot: eso es exactamente lo que separa las dos listas.
            query = '''
                SELECT le.lead_id, le.responsible_user_id, le.lead_nombre, le.asesor_nombre,
                       MAX(ma.ts) AS f_ult_msj_asesor,
                       MAX(ma.ts) FILTER (
                           WHERE NOT (COALESCE(ma.autor_nombre, '') = ANY(%s))
                       ) AS ultimo_de_persona
                  FROM mensajes_asesor ma
                  JOIN leads_estado le
                    ON le.subdomain = ma.subdomain AND le.lead_id = ma.lead_id
                 WHERE ma.subdomain = %s AND ma.ts >= %s AND ma.ts < %s
            '''
            params = [autos, subdomain, inicio, fin]
        else:
            query = '''
                SELECT lead_id, responsible_user_id, lead_nombre, asesor_nombre,
                       f_ult_msj_asesor, f_ult_msj_asesor AS ultimo_de_persona
                  FROM leads_estado
                 WHERE subdomain = %s
                   AND f_ult_msj_asesor >= %s AND f_ult_msj_asesor < %s
            '''
            params = [subdomain, inicio, fin]

        if responsible_user_ids:
            query += ' AND responsible_user_id = ANY(%s)'
            params.append(list(responsible_user_ids))
        if solo_abiertas:
            query += ' ' + FILTRO_SOLO_ABIERTAS
        if sql_embudo:
            query += sql_embudo
            params.extend(params_embudo)
        if corte:
            query += (' GROUP BY le.lead_id, le.responsible_user_id, '
                      'le.lead_nombre, le.asesor_nombre')
        query += ' ORDER BY 5 DESC'      # por f_ult_msj_asesor
        c.execute(query, params)
        rows = c.fetchall()
    finally:
        if propia:
            conn.close()

    por_persona, solo_bot = [], []
    for r in rows:
        # El horario se mide sobre la respuesta que cuenta para esa lista: si
        # la persona contesto 09:00 y el bot 22:00, el lead se atendio en
        # horario y no deberia quedar afuera por el mensaje del bot.
        # Si la columna no viniera, se cae al comportamiento viejo —todo cuenta
        # como atendido por una persona— en vez de tirar 500. Separar mal es
        # malo; dejar el tablero sin snapshot es peor.
        up = r.get('ultimo_de_persona', r['f_ult_msj_asesor'])
        de_persona = up is not None
        ts = up if de_persona else r['f_ult_msj_asesor']
        if not (h_ini <= _ts_to_local(ts, tz_offset).hour < h_fin):
            continue
        fila = dict(r)
        fila['f_ult_msj_asesor'] = ts
        (por_persona if de_persona else solo_bot).append(
            _fmt_lead_estado(fila, tz_offset, 'f_ult_msj_asesor'))
    return por_persona, solo_bot

def _lista_asesores(subdomain, conn=None):
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            'SELECT DISTINCT responsible_user_id FROM leads_estado WHERE subdomain = %s AND responsible_user_id IS NOT NULL',
            (subdomain,)
        )
        ids = [row[0] for row in c.fetchall()]
    finally:
        if propia:
            conn.close()
    asesores = [{'id': uid, 'nombre': nombre_asesor(uid)} for uid in ids]
    asesores = [a for a in asesores if a['nombre']]
    return sorted(asesores, key=lambda x: x['nombre'])

def _lista_asesores_campo(subdomain, conn=None):
    """Nombres vistos en el campo custom "Asesor" de esta cuenta.

    Se sacan de los datos y no de una lista fija: es texto que se carga en
    Kommo, asi que entra gente nueva sin avisar. Si la cuenta no tiene el campo
    configurado devuelve vacio y el dashboard no muestra el filtro."""
    if not campo_quien_atendio_de(subdomain):
        return []
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            'SELECT DISTINCT asesor_nombre FROM tiempos_respuesta '
            'WHERE subdomain = %s AND asesor_nombre IS NOT NULL',
            (subdomain,)
        )
        nombres = [row[0] for row in c.fetchall() if (row[0] or '').strip()]
    finally:
        if propia:
            conn.close()
    return sorted(nombres)

def _cobertura(subdomain, desde_str, hasta_str, dias_lab, conn=None):
    """Que porcion de los dias LABORABLES del periodo elegido tiene datos.

    Cuando el webhook se cae dejamos de recibir mensajes, pero el dashboard
    igual muestra numeros para esos dias: 'F Ult msj cliente' arrastra fechas
    viejas, asi que un lead que alguien toco en julio genera una fila fechada
    en junio. El resultado es una muestra sesgada — solo los leads que
    volvieron a moverse — con el mismo aspecto que una completa.

    Devolver la cobertura permite decirlo en pantalla en vez de que el hueco
    pase por dato."""
    if not desde_str or not hasta_str:
        return None
    try:
        desde = datetime.strptime(desde_str, '%Y-%m-%d').date()
        hasta = datetime.strptime(hasta_str, '%Y-%m-%d').date()
    except ValueError:
        return None
    if desde > hasta:
        return None

    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            '''SELECT DISTINCT LEFT(capturado_at, 10)
               FROM eventos
               WHERE subdomain = %s AND capturado_at >= %s AND capturado_at < %s''',
            (subdomain, desde_str, (hasta + timedelta(days=1)).strftime('%Y-%m-%d'))
        )
        con_datos = {r[0] for r in c.fetchall()}
    finally:
        if propia:
            conn.close()

    laborables, cubiertos = 0, 0
    d = desde
    hoy = datetime.utcnow().date()
    while d <= hasta and d <= hoy:
        if d.weekday() in dias_lab:
            laborables += 1
            if d.strftime('%Y-%m-%d') in con_datos:
                cubiertos += 1
        d += timedelta(days=1)

    if not laborables:
        return None
    return {
        'dias_laborables': laborables,
        'dias_con_datos':  cubiertos,
        'pct':             round(cubiertos / laborables * 100),
    }

def _fmt_dias(dias_lab):
    """Los dias laborables como rango: "Lun a Vie" en vez de
    "Lun · Mar · Mié · Jue · Vie".

    La lista con puntos se leia como opciones seleccionables, no como el
    horario que es. Cuando los dias no son consecutivos —que puede pasar— se
    cae a la lista, que ahi si es la unica forma de decirlo."""
    dias = sorted(dias_lab)
    if not dias:
        return '—'
    if len(dias) == 1:
        return DIAS_LABEL[dias[0]]
    consecutivos = dias == list(range(dias[0], dias[-1] + 1))
    if consecutivos:
        return f'{DIAS_LABEL[dias[0]]} a {DIAS_LABEL[dias[-1]]}'
    return ' · '.join(DIAS_LABEL[d] for d in dias)

# Un dia laborable suelto sin eventos puede ser un feriado o un dia flojo.
# Dos o mas seguidos ya no: eso es el webhook caido.
MAX_DIAS_SIN_DATOS = 1

# Cache de _fecha_minima por cuenta. La consulta recorre TODOS los eventos de
# la cuenta —392.000 en Camara China, 0,8 s medidos— para un dato que cambia
# una vez al dia: el primer dia de la racha continua de captura. Una hora de
# vigencia alcanza y sobra; si el webhook se cae y vuelve, el piso se mueve a
# lo sumo una hora despues, y la caida ya la avisa /health/actividad.
_FECHA_MIN_TTL_SEG = 3600
_fecha_min_cache = {}     # subdomain -> (valor, expira_en)

def _fecha_minima(subdomain, conn=None):
    """Version con cache de _fecha_minima_calc. Ver ahi que calcula."""
    ahora = time.monotonic()
    v = _fecha_min_cache.get(subdomain)
    if v and v[1] > ahora:
        return v[0]
    valor = _fecha_minima_calc(subdomain, conn)
    _fecha_min_cache[subdomain] = (valor, ahora + _FECHA_MIN_TTL_SEG)
    return valor

def _fecha_minima_calc(subdomain, conn=None):
    """Desde cuando la captura viene SIN CORTES para este cliente.

    Antes devolvia MIN(capturado_at): el primer evento que recibimos alguna
    vez. Pero si despues hubo semanas sin recibir nada, ese piso miente. El
    dashboard ofrecia elegir junio como si tuviera datos, cuando tres de los
    cuatro clientes estuvieron oscuros desde fines de mayo hasta el 13 de
    julio: los numeros de junio salian solo de los leads que volvieron a
    moverse en julio, una muestra sesgada con aspecto de completa.

    Se busca entonces el arranque de la racha continua mas reciente, contando
    solo dias laborables. Se calcula, no se fija a mano, para que la proxima
    caida se acomode sola.

    Sigue usando capturado_at y no el timestamp del evento: lo que importa es
    cuando NOSOTROS recibimos, no la fecha que trae el lead (un lead dormido
    puede llegar con fecha de hace anios en el snapshot inicial)."""
    dias_lab = PULSE_CONFIG.get(subdomain, {}).get('dias_laborables', [0, 1, 2, 3, 4])
    # Usa la conexion que le pasan, o abre una propia. Cada conexion a
    # Supabase cuesta ~0,35 s de saludo TLS: el tablero abria seis por carga.
    propia = conn is None
    if propia:
        conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            'SELECT DISTINCT LEFT(capturado_at, 10) FROM eventos WHERE subdomain = %s',
            (subdomain,)
        )
        con_datos = {r[0] for r in c.fetchall() if r[0]}
    finally:
        if propia:
            conn.close()
    if not con_datos:
        return None

    try:
        d = datetime.strptime(max(con_datos), '%Y-%m-%d').date()
        primero = datetime.strptime(min(con_datos), '%Y-%m-%d').date()
    except ValueError:
        return min(con_datos)

    # Se camina hacia atras desde el ultimo dia con datos y se corta al primer
    # hueco real.
    candidato, faltantes = d, 0
    while d >= primero:
        if d.weekday() in dias_lab:
            if d.strftime('%Y-%m-%d') in con_datos:
                candidato, faltantes = d, 0
            else:
                faltantes += 1
                if faltantes > MAX_DIAS_SIN_DATOS:
                    break
        d -= timedelta(days=1)
    return candidato.strftime('%Y-%m-%d')

@app.route('/pulse', methods=['GET', 'POST'])
def pulse():
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain:
        return "Accede con ?subdomain=nombre", 400

    if request.method == 'POST':
        pw_real = pulse_password(subdomain)
        if pw_real and request.form.get('password', '') == pw_real:
            session[f'pulse_ok_{subdomain}'] = True
            return redirect(url_for('pulse', subdomain=subdomain))
        return render_template('pulse_login.html', subdomain=subdomain, error='Contraseña incorrecta'), 401

    if not pulse_autorizado(subdomain):
        if not pulse_password(subdomain):
            return f'PULSE no tiene contraseña configurada para "{subdomain}" (falta PULSE_PW_{subdomain.upper()})', 500
        return render_template('pulse_login.html', subdomain=subdomain, error=None)

    # El "hoy" para el rango por defecto sale de la zona de la CUENTA, no del
    # navegador. Con toISOString() el rango arrancaba en UTC y en El Salvador
    # (-6) a partir de las 18:00 locales el dashboard ya ofrecia la fecha de
    # mañana. Ademas la cuenta puede estar en otra zona que quien la mira.
    tz = PULSE_CONFIG.get(subdomain, {}).get('tz_offset', 0)
    hoy_cuenta = (datetime.utcnow() + timedelta(hours=tz)).strftime('%Y-%m-%d')
    # La paleta baja del backend en vez de estar escrita a mano en el CSS: asi
    # un color se cambia en un solo lugar y cambia en los tres lados.
    return render_template('pulse.html', subdomain=subdomain, hoy_cuenta=hoy_cuenta,
                           paleta_css=paleta.para_css(), paleta_js=paleta.para_js())

@app.route('/pulse/propuesta')
def pulse_propuesta():
    """Version de prueba de los tres cambios de graficos, con datos reales.

    Existe para que el cliente los vea ANTES de tocar el dashboard: son
    elementos que mira todos los dias, y enterarse del cambio abriendo la
    pantalla es la peor forma de enterarse.

    Usa /pulse/data, o sea los mismos numeros. Lo unico que cambia es como se
    dibujan. Y pide la misma contraseña que el dashboard: muestra los mismos
    datos de clientes."""
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain or subdomain not in PULSE_CONFIG:
        return f'Subdomain "{subdomain}" no configurado en PULSE', 400
    if not pulse_autorizado(subdomain):
        return redirect(url_for('pulse', subdomain=subdomain))
    return render_template('pulse_propuesta.html', subdomain=subdomain)

@app.route('/pulse/logout')
def pulse_logout():
    subdomain = request.args.get('subdomain', '').strip()
    session.pop(f'pulse_ok_{subdomain}', None)
    return redirect(url_for('pulse', subdomain=subdomain))

@app.route('/pulse/snapshot')
def pulse_snapshot():
    """Solo las dos tarjetas de "ahora mismo", para refrescarlas solas.

    Existe para no reconsultar el dashboard entero cada minuto y medio. El
    resto de la pantalla depende del rango de fechas y no cambia si nadie toca
    el filtro; el snapshot si cambia, y hasta ahora habia que recargar la
    pagina para enterarse.

    Recibe los mismos filtros que /pulse/data. Si recibiera otros, el numero
    de la tarjeta dejaria de coincidir con la lista que se abre debajo."""
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain or subdomain not in PULSE_CONFIG:
        return jsonify({'error': 'subdomain no configurado'}), 400
    if not pulse_autorizado(subdomain):
        return jsonify({'error': 'No autorizado'}), 401

    cfg       = PULSE_CONFIG[subdomain]
    tz_offset = cfg['tz_offset']
    h_ini, h_fin = cfg['horario']

    asesor_ids = sorted({
        int(p) for crudo in request.args.getlist('asesor')
        for p in str(crudo).split(',') if p.strip().lstrip('-').isdigit()
    })
    solo_abiertas = request.args.get('abiertas', '').strip() == '1'
    embudos_sel = sorted({
        int(p.strip()) for crudo in request.args.getlist('embudo')
        for p in str(crudo).split(',') if p.strip().isdigit()
    })
    etapas_sel = sorted({
        p.strip() for crudo in request.args.getlist('etapa')
        for p in str(crudo).split(',') if p.strip() and ':' in p
    })
    sql_embudo, params_embudo = _filtro_embudo(embudos_sel, etapas_sel)

    # UNA conexion para todo el refresco. Abria cinco —corte, fecha minima,
    # sin responder, atendidos, nombres— y cada una paga ~0,35 s de saludo TLS
    # contra Supabase; y esto corre cada 90 segundos por pestaña abierta.
    conn = get_conn()
    # Solo lecturas, y cada una suelta su bloqueo al terminar. Sin esto la
    # conexion compartida retiene los bloqueos de lectura de cada tabla hasta
    # el final, y se cruza con el init_db del proceso nuevo en cada deploy.
    conn.autocommit = True
    try:
        corte = _corte_salientes(conn.cursor(cursor_factory=RealDictCursor), subdomain)
        fecha_min = _fecha_minima(subdomain, conn)
        piso = _local_date_to_ts(fecha_min, tz_offset) if fecha_min else None

        bloque = _bloque_ahora(subdomain, tz_offset, h_ini, h_fin, corte, piso, conn,
                               asesor_ids, solo_abiertas, sql_embudo, params_embudo)
    finally:
        conn.close()
    return jsonify(bloque)

@app.route('/pulse/data')
def pulse_data():
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain or subdomain not in PULSE_CONFIG:
        return jsonify({'error': f'Subdomain "{subdomain}" no configurado en PULSE'}), 400
    if not pulse_autorizado(subdomain) and not _token_admin_ok():
        return jsonify({'error': 'No autorizado'}), 401

    # Perfil de tiempos: cuanto tarda cada tramo de esta respuesta, medido en
    # el servidor. Se devuelve con ?perfil=1. Existe porque "el tablero tarda"
    # no dice si es la base, la red, el calculo o el navegador, y cada uno se
    # arregla distinto. Se mide siempre (cuesta nada) y se muestra si se pide.
    perfil = request.args.get('perfil') == '1'
    tiempos = {}
    _t = [time.perf_counter()]
    def _marca(nombre):
        ahora = time.perf_counter()
        tiempos[nombre] = round(ahora - _t[0], 3)
        _t[0] = ahora

    cfg       = PULSE_CONFIG[subdomain]
    tz_offset = cfg['tz_offset']
    h_ini, h_fin = cfg['horario']
    dias_lab  = set(cfg['dias_laborables'])
    franjas   = cfg['franjas']

    desde_str = request.args.get('desde')
    hasta_str = request.args.get('hasta')
    # Varios responsables a la vez (pedido de Ana). Se acepta repetido
    # (?asesor=1&asesor=2) y separado por comas, porque la URL la arma el
    # dashboard pero tambien la pega gente a mano.
    asesor_ids = []
    for crudo in request.args.getlist('asesor'):
        for parte in str(crudo).split(','):
            parte = parte.strip()
            if not parte:
                continue
            try:
                asesor_ids.append(int(parte))
            except ValueError:
                # Un id invalido se ignora en vez de romper el dashboard: el
                # filtro es una comodidad, no algo por lo que valga un 500.
                pass
    asesor_ids = sorted(set(asesor_ids))

    # Filtro por el campo custom "Asesor": son nombres, no ids, porque es texto
    # libre cargado en Kommo y no hay un id detras.
    asesores_nombre = sorted({
        p.strip()
        for crudo in request.args.getlist('asesor_nombre')
        for p in str(crudo).split('||')      # '||' y no coma: hay apellidos compuestos
        if p.strip()
    })
    fuera_horario = request.args.get('modo', 'laboral').strip() == 'fuera_horario'
    solo_abiertas = request.args.get('abiertas', '').strip() == '1'

    # Embudos tildados enteros, y etapas sueltas como "pipeline:status".
    embudos_sel = []
    for crudo in request.args.getlist('embudo'):
        for p in str(crudo).split(','):
            if p.strip().isdigit():
                embudos_sel.append(int(p.strip()))
    embudos_sel = sorted(set(embudos_sel))
    etapas_sel = sorted({
        p.strip() for crudo in request.args.getlist('etapa')
        for p in str(crudo).split(',')
        if p.strip() and ':' in p
    })
    sql_embudo, params_embudo = _filtro_embudo(embudos_sel, etapas_sel)

    desde_ts = _local_date_to_ts(desde_str, tz_offset) if desde_str else None
    hasta_ts = (_local_date_to_ts(hasta_str, tz_offset) + 86399) if hasta_str else None

    _marca('parametros')
    conn = get_conn()
    # Solo lecturas. Con autocommit cada consulta suelta su bloqueo al
    # terminar, como cuando cada una tenia su conexion. Sin esto, la conexion
    # compartida retiene los bloqueos de lectura de todas las tablas que ya
    # leyo hasta el final de la carga (~3 s), y en un deploy se cruza con los
    # ALTER de init_db del proceso nuevo: "deadlock detected", 04/09.
    conn.autocommit = True
    _marca('conexion')
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        # Frontera entre los dos metodos de medicion. Ver _corte_salientes.
        corte = _corte_salientes(c, subdomain)
        _marca('corte')
        # DISTINCT ON: si el asesor manda varios mensajes seguidos respondiendo
        # al MISMO mensaje del cliente, cada uno actualiza f_ult_msj_asesor y
        # generaba una fila "respuesta" separada, inflando el conteo. Nos
        # quedamos solo con la primera respuesta (f_ult_msj_asesor mas chico)
        # por cada mensaje distinto del cliente.
        # inicio_espera: desde cuando espera REALMENTE el cliente. Es el primer
        # mensaje suyo sin responder; 'F Ult msj cliente' guarda el ULTIMO, y
        # medir desde ahi subestima la espera (medido en produccion: 2.3x en la
        # mediana, 3.1x en el p90).
        #
        # Se toma el MAS TEMPRANO de las dos fuentes, no solo el nuestro. Las
        # dos son validas y ninguna es completa: si Kommo dice que el cliente
        # escribio 08:30 y el primer mensaje que capturamos es 09:15, el cliente
        # empezo a esperar 08:30 — ese mensaje existio aunque no lo hayamos
        # guardado. En 20 de 100 filas revisadas de corepowerconsulting nuestro
        # dato llegaba tarde, subestimando 27 minutos en la mediana y hasta 45.
        # LEAST ignora NULL, asi que cubre solo el caso de tener uno de los dos.
        query = '''
            SELECT DISTINCT ON (lead_id, f_ult_msj_cliente)
                lead_id, f_ult_msj_cliente, f_ult_msj_asesor, responsible_user_id, lead_nombre,
                asesor_nombre,
                f_primer_msj_cliente,
                LEAST(f_primer_msj_cliente, f_ult_msj_cliente) AS inicio_espera
            FROM tiempos_respuesta
            WHERE subdomain = %s
              AND f_ult_msj_cliente IS NOT NULL
              AND f_ult_msj_asesor  IS NOT NULL
              AND f_ult_msj_asesor > f_ult_msj_cliente
              AND (f_ult_msj_asesor - f_ult_msj_cliente) <= %s
        '''
        params = [subdomain, GAP_REACTIVACION_SEG]
        # Desde el corte manda el mensaje real y no el campo de fecha. El "<"
        # es lo que evita contar dos veces: cada respuesta cae en un metodo y
        # en uno solo, partida por CUANDO se respondio.
        if corte:
            query += ' AND f_ult_msj_asesor < %s'
            params.append(corte)
        if desde_ts is not None:
            query += ' AND LEAST(f_primer_msj_cliente, f_ult_msj_cliente) >= %s'
            params.append(desde_ts)
        if hasta_ts is not None:
            query += ' AND LEAST(f_primer_msj_cliente, f_ult_msj_cliente) <= %s'
            params.append(hasta_ts)
        if asesor_ids:
            query += ' AND responsible_user_id = ANY(%s)'
            params.append(asesor_ids)
        # Filtro por quien atendio de verdad. Es independiente del de
        # responsable: en Camara China los dos responden preguntas distintas
        # —"que hizo el usuario Ventas" contra "que hizo Ivis"— y se pueden
        # combinar.
        if asesores_nombre:
            query += ' AND asesor_nombre = ANY(%s)'
            params.append(asesores_nombre)
        # El embudo vive en leads_estado, no en tiempos_respuesta: se acota con
        # una subconsulta, igual que hace el interruptor de "solo abiertas".
        if sql_embudo:
            query += (' AND lead_id IN (SELECT lead_id FROM leads_estado '
                      'WHERE subdomain = %s' + sql_embudo + ')')
            params.append(subdomain)
            params.extend(params_embudo)
        query += ' ORDER BY lead_id, f_ult_msj_cliente, f_ult_msj_asesor ASC'
        # Si el periodo entero es posterior al corte, esta consulta no puede
        # devolver nada: mide solo respuestas ANTERIORES al corte
        # (f_ult_msj_asesor < corte), y el inicio de la espera es siempre
        # menor que la respuesta. Se salta: eran 0,6 s en Camara China para
        # traer una lista vacia, y es el caso comun porque el rango por
        # defecto es reciente.
        if corte and desde_ts is not None and desde_ts >= corte:
            rows = []
        else:
            c.execute(query, params)
            rows = c.fetchall()

        # Nombre de respaldo para los leads que Kommo nunca bautizo. Se piden
        # SOLO los que hacen falta: gruporegalado tiene decenas de miles de
        # mensajes y traerlos todos para completar unos cientos de nombres
        # cargaria la consulta del dashboard sin necesidad.
        #
        # DISTINCT ON ... ORDER BY ts DESC toma el nombre mas reciente: si el
        # contacto se renombro en Kommo, vale el ultimo.
        # Lo que ocurrio DESDE el corte se mide con los mensajes propios.
        _marca('consulta_campos')
        registros_msj = _registros_de_mensajes(
            c, subdomain, corte, tz_offset, h_ini, h_fin, dias_lab,
            fuera_horario, desde_ts, hasta_ts, asesor_ids, asesores_nombre,
            embudos_sel, etapas_sel)
        _marca('registros_mensajes')

        # Lo que se ofrece en el selector: solo embudos con leads de verdad.
        lista_embudos = _embudos_con_leads(c, subdomain)
        _marca('embudos')

        nombres_cliente = {}
        faltantes = list(
            {r['lead_id'] for r in rows if not (r['lead_nombre'] or '').strip()}
            | {r['lead_id'] for r in registros_msj if not r['lead_nombre']})
        if faltantes:
            # Aca se reusa el cursor ya abierto en vez de _completar_nombres,
            # que abre su propia conexion: estamos adentro del bloque que ya
            # tiene una.
            c.execute(SQL_NOMBRES_CLIENTE, (subdomain, faltantes))
            nombres_cliente = {r['lead_id']: r['cliente_nombre'] for r in c.fetchall()}
        _marca('nombres_cliente')
    except Exception as e:
        print(f'❌ PULSE /data query: {e}')
        conn.close()
        return jsonify({'error': str(e)}), 500
    # La conexion sigue ABIERTA a proposito: la usan tambien fecha minima, sin
    # responder, atendidos hoy, nombres y cobertura, mas abajo. Antes cada una
    # abria la suya —seis por carga— y cada conexion a Supabase paga ~0,35 s
    # de saludo TLS: 1,8 s de una carga de 6 eran apretones de manos. Se cierra
    # en el finally del bloque que sigue.

    try:
        registros = []
        con_inicio_real = 0
        for row in rows:
            inicio = row['inicio_espera']
            dt_cliente = _ts_to_local(inicio, tz_offset)
            en_horario = _en_horario_laboral(dt_cliente, h_ini, h_fin, dias_lab)

            if fuera_horario:
                # Solo mensajes que llegaron FUERA del horario configurado,
                # medidos en tiempo real de reloj (sin saltar horas no
                # laborables, al reves del calculo de "tiempo efectivo").
                if en_horario:
                    continue
                efectivo = row['f_ult_msj_asesor'] - inicio
            else:
                efectivo = _calc_tiempo_efectivo(
                    inicio, row['f_ult_msj_asesor'],
                    tz_offset, h_ini, h_fin, dias_lab
                )

            if row['f_primer_msj_cliente'] is not None:
                con_inicio_real += 1

            registros.append({
                'lead_id':          row['lead_id'],
                # El nombre propio del lead manda: "Mariano Hernandez -
                # Tradicion Burguer" dice mas que solo el nombre del contacto.
                # El del mensaje entra unicamente cuando el lead no tiene.
                'lead_nombre':      (row['lead_nombre'] or '').strip()
                                    or nombres_cliente.get(row['lead_id']),
                'inicio_espera':    inicio,
                'efectivo_seg':      efectivo,
                'responsible_user_id': row['responsible_user_id'],
                'asesor_nombre':    (row['asesor_nombre'] or '').strip() or None,
            })

        # Las filas medidas con los mensajes se suman a las viejas. No se
        # solapan: la consulta vieja se corto en f_ult_msj_asesor < corte y
        # estas empiezan ahi.
        for r in registros_msj:
            if not r['lead_nombre']:
                r['lead_nombre'] = nombres_cliente.get(r['lead_id'])
        registros.extend(registros_msj)

        # El numero grande, la distribucion y la tendencia miden la atencion de
        # una PERSONA. Los turnos que cerro un bot se sacan aca y no antes,
        # porque el ranking si los muestra: se pidio ver al bot como un asesor
        # mas, con su volumen y su tiempo.
        #
        # Si entraran al numero grande lo volverian inutil: en tucoytico el
        # Salesbot contesta en 4 segundos y son 505 de 1.215 turnos, asi que la
        # mediana caeria de 1h 01m a 3m 38s. Seria cierto y no querria decir
        # nada, porque el cliente sigue esperando a una persona.
        registros_persona = [r for r in registros if not r.get('respondio_auto')]

        # Consolidado POR LEAD, no por respuesta. Antes cada respuesta era una
        # fila, asi que un mismo lead aparecia varias veces en la lista: en
        # corepowerconsulting el lead 9872182 ocupaba tres de los cinco puestos.
        # Eso no informa nada — es la misma conversacion en curso, y en una
        # conversacion los tiempos varian segun cuando conteste el cliente.
        #
        # Se promedian las ULTIMAS respuestas de cada lead: describe como se
        # viene atendiendo esa conversacion en vez de un caso suelto, que puede
        # ser un pico aislado.
        por_lead = defaultdict(list)
        for r in registros_persona:
            por_lead[r['lead_id']].append(r)

        consolidado = []
        for rs in por_lead.values():
            rs.sort(key=lambda x: x['inicio_espera'], reverse=True)
            ultimas = rs[:TOP_ULTIMAS_RESPUESTAS]
            fila = dict(ultimas[0])          # la mas reciente: nombre, fecha, asesor
            # Mediana y no promedio (pedido de Ana): con 5 valores, una sola
            # respuesta muy lenta arrastra el promedio y el lead aparece entre
            # los peores por un caso aislado. La mediana describe como se viene
            # atendiendo la conversacion. Es el mismo criterio que ya usa el
            # ranking por responsable.
            fila['efectivo_seg'] = _pctl([x['efectivo_seg'] for x in ultimas], 50)
            fila['respuestas']   = len(ultimas)
            consolidado.append(fila)

        # Se filtra ANTES de ordenar y cortar: filtrar despues del top 10
        # dejaria listas de dos o tres filas sin motivo visible.
        comparables = [r for r in consolidado
                       if r['respuestas'] >= MIN_RESPUESTAS_EXTREMOS]
        top_lentos  = sorted(comparables, key=lambda r: r['efectivo_seg'], reverse=True)[:10]
        top_rapidos = sorted(comparables, key=lambda r: r['efectivo_seg'])[:10]

        daily = defaultdict(list)
        daily_asesor = defaultdict(lambda: defaultdict(list))
        for r in registros_persona:
            dia = _ts_to_local(r['inicio_espera'], tz_offset).strftime('%Y-%m-%d')
            daily[dia].append(r['efectivo_seg'])
            # Se guardan los segundos y no un contador: con la lista se puede
            # sacar la cantidad Y la mediana del dia de cada persona, que es lo
            # que hace falta para comparar un dia contra otro.
            #
            # Apilar por responsable pierde sentido con UNO solo elegido: seria
            # una sola serie del mismo color que el total. Con varios sigue
            # sirviendo, que es justo para lo que se pidio la seleccion multiple.
            if len(asesor_ids) != 1:
                daily_asesor[dia][asesor_de_registro(r) or 'Sin asignar'].append(
                    r['efectivo_seg'])

        dias_ordenados = sorted(daily.items())[-30:]
        # mediana_seg y no solo el promedio: el numero grande de la pantalla es
        # la mediana, y la linea del grafico venia dibujando el promedio sobre
        # los mismos datos. Quien comparaba los dos numeros veia una diferencia
        # de horas y concluia que uno estaba roto. El promedio se sigue mandando
        # para el tooltip, donde ver los dos juntos si informa.
        tendencia = [
            {
                'fecha':      dia,
                'mediana_seg': _pctl(v, 50),
                'promedio_seg': round(sum(v) / len(v)),
                'promedio_h': round(sum(v) / len(v) / 3600, 1),
                'total':      len(v),
            }
            for dia, v in dias_ordenados
        ]

        tendencia_por_asesor = None
        if len(asesor_ids) != 1:
            dias = [dia for dia, _ in dias_ordenados]
            nombres = sorted({n for dia in dias for n in daily_asesor[dia]})

            # Con siete series apiladas no se puede seguir ninguna: el ojo no
            # distingue siete colores en barras de 20px. Se dejan las que mas
            # respondieron y el resto se junta en "Otros", que sigue sumando al
            # total pero no agrega una raya de color mas.
            if len(nombres) > MAX_SERIES_TENDENCIA:
                por_volumen = sorted(
                    nombres,
                    key=lambda n: -sum(len(daily_asesor[d].get(n, [])) for d in dias))
                principales = por_volumen[:MAX_SERIES_TENDENCIA]
                resto = set(por_volumen[MAX_SERIES_TENDENCIA:])
                for dia in dias:
                    juntos = [s for n in resto for s in daily_asesor[dia].get(n, [])]
                    for n in resto:
                        daily_asesor[dia].pop(n, None)
                    if juntos:
                        # Se juntan los SEGUNDOS, no las medianas: promediar
                        # medianas de personas distintas no da ningun numero.
                        daily_asesor[dia][OTROS] = juntos
                nombres = principales + ([OTROS] if any(OTROS in daily_asesor[d] for d in dias) else [])
            # Los colores —y el porque de cada uno— viven en paleta.py.
            # Cada serie lleva las dos lecturas del mismo dia:
            #   data       cuantas respondio  -> barras apiladas
            #   mediana_seg cuanto tardo      -> una linea por persona
            #
            # Las medianas NO se apilan: sumar la mediana de dos personas no da
            # ningun numero con sentido. Por eso el modo "tiempo" del grafico
            # dibuja lineas y no barras.
            #
            # None cuando esa persona no respondio nada ese dia: asi la linea
            # queda cortada en vez de bajar a cero y simular que atendio al
            # instante.
            tendencia_por_asesor = [
                {
                    'asesor': nombre,
                    # "Otros" en gris: no es una persona, no compite por
                    # atencion con las que si lo son.
                    'color': (COLOR_OTROS if nombre == OTROS
                              else PALETA_ASESORES[i % len(PALETA_ASESORES)]),
                    'data': [len(daily_asesor[dia].get(nombre, [])) for dia in dias],
                    'mediana_seg': [
                        _pctl(daily_asesor[dia][nombre], 50)
                        if daily_asesor[dia].get(nombre) else None
                        for dia in dias
                    ],
                }
                for i, nombre in enumerate(nombres)
            ]

        _marca('calculo')
        # Con cache de una hora: la consulta recorre todos los eventos.
        fecha_min = _fecha_minima(subdomain, conn)
        _marca('fecha_minima')
        piso_captura = _local_date_to_ts(fecha_min, tz_offset) if fecha_min else None

        # Todo el bloque de operacion —las dos listas, su comparativo y que
        # dia se esta mirando— en una sola funcion, la misma que usa
        # /pulse/snapshot. Con ?dia= el bloque entero pasa a esa fecha.
        # Las listas salen de leads_estado, que no tiene el nombre del
        # cliente; _bloque_ahora lo completa adentro.
        bloque = _bloque_ahora(subdomain, tz_offset, h_ini, h_fin, corte, piso_captura, conn,
                               asesor_ids, solo_abiertas, sql_embudo, params_embudo)
        no_respondidos     = bloque['no_respondidos']
        trabajados_hoy     = bloque['trabajados_hoy']
        atendidos_solo_bot = bloque['atendidos_solo_bot']
        comparado          = bloque['comparado']
        _marca('bloque_ahora')
        cobertura = _cobertura(subdomain, desde_str, hasta_str, dias_lab, conn=conn)
        _marca('cobertura')
        tiempos['total'] = round(sum(tiempos.values()), 3)

        return jsonify({
            **({'tiempos': tiempos} if perfil else {}),
            'config': {
                'nombre':      cfg['nombre'],
                'horario_fmt': _fmt_horario(h_ini, h_fin),
                'dias_fmt':    _fmt_dias(dias_lab),
                'franjas':     franjas,
                'crm_url':     cfg.get('crm_domain') and f'https://{cfg["crm_domain"]}',
                'asesores':    _lista_asesores(subdomain, conn=conn),
                # Solo se nombra cuando hay UNO: con varios elegidos el titulo
                # tendria que enumerarlos y no cabe.
                'asesor_actual': nombre_asesor(asesor_ids[0]) if len(asesor_ids) == 1 else None,
                'asesores_elegidos': asesor_ids,
                # Vacio en las cuentas sin campo "Asesor": el dashboard usa eso
                # para no mostrar un filtro que no filtraria nada.
                'asesores_campo': _lista_asesores_campo(subdomain, conn=conn),
                # Selector anidado: el embudo es el padre y sus etapas los
                # hijos. Vacio en las cuentas sin pipeline_id todavia cargado,
                # y ahi el dashboard no muestra el filtro.
                'embudos':           lista_embudos,
                'embudos_elegidos':  embudos_sel,
                'etapas_elegidas':   etapas_sel,
                'fecha_minima': fecha_min,
                # Dias que no se pueden reconstruir: el selector no los ofrece,
                # porque el numero de esos dias seria falso.
                'dias_parciales': sorted(dias_parciales(subdomain, conn)),
                'modo':        'fuera_horario' if fuera_horario else 'laboral',
                'min_muestra_asesor': MIN_MUESTRA_ASESOR,
                # Solo para explicar una lista vacia en pantalla.
                'min_respuestas_extremos': MIN_RESPUESTAS_EXTREMOS,
                'cobertura': cobertura,
                # Que porcion de los registros mide desde el PRIMER mensaje sin
                # responder. El resto cae al ultimo mensaje del cliente y por lo
                # tanto subestima la espera: sirve para saber desde que fecha
                # los numeros son comparables.
                'medicion': {
                    'total':           len(registros_persona),
                    'con_inicio_real': con_inicio_real,
                    'pct_inicio_real': round(con_inicio_real / len(registros) * 100, 1) if registros else 0,
                    # Desde aca la respuesta sale del mensaje real y no del
                    # campo de fecha. El dashboard lo marca en la tendencia:
                    # sin la marca, el escalon de los numeros se lee como una
                    # mejora del equipo y no como un cambio de instrumento.
                    'corte_mensajes': corte,
                    'corte_fecha': (_ts_to_local(corte, tz_offset).strftime('%Y-%m-%d')
                                    if corte else None),
                    'respuestas_de_bot': len(registros) - len(registros_persona),
                },
            },
            'metricas':      _calc_metricas(registros_persona, franjas, tz_offset),
            'top_lentos':    [_fmt_row(r, tz_offset) for r in top_lentos],
            'top_rapidos':   [_fmt_row(r, tz_offset) for r in top_rapidos],
            'tendencia':     tendencia,
            'tendencia_por_asesor': tendencia_por_asesor,
            # Se calcula SIEMPRE, tambien con un responsable elegido. Antes se
            # devolvia None en ese caso y el dashboard escondia la tarjeta
            # entera: al filtrar por una persona desaparecia su mediana y su
            # p90, que es justo lo que uno quiere ver de esa persona. Con un
            # solo responsable devuelve una fila.
            'por_asesor':    _calc_por_asesor(registros, franjas),
            'no_respondidos': no_respondidos,
            'trabajados_hoy': trabajados_hoy,
            # Los que solo recibieron el saludo del bot. Es el punto ciego del
            # tablero: dejan de figurar en "Sin responder" porque el ultimo
            # mensaje es nuestro, y figuraban en "Atendidos" sin que ninguna
            # persona los hubiera mirado.
            'atendidos_solo_bot': atendidos_solo_bot,
            # Mismo comparativo que en /pulse/snapshot, para que la primera
            # carga ya lo traiga y no haya que esperar al refresco.
            'comparado': comparado,
            'dia': bloque['dia'],
        })
    except Exception as e:
        print(f'❌ PULSE /data procesamiento: {e}')
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ========================
# MAIN
# ========================
if __name__ == '__main__':
    print("🚀 Servidor corriendo en puerto 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)

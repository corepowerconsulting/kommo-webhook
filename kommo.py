from flask import Flask, request, jsonify, render_template, session, redirect, url_for
import json
import re
import secrets
import threading
from datetime import datetime, timedelta
import calendar
from collections import defaultdict
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from pulse_config import PULSE_CONFIG, ASESORES

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

# ========================
# BASE DE DATOS
# ========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    conn = get_conn()
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
    c.execute('ALTER TABLE tiempos_respuesta ADD COLUMN IF NOT EXISTS responsible_user_id BIGINT')
    c.execute('ALTER TABLE tiempos_respuesta ADD COLUMN IF NOT EXISTS lead_nombre TEXT')
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
    c.execute('ALTER TABLE leads_estado ADD COLUMN IF NOT EXISTS lead_nombre TEXT')
    conn.commit()
    conn.close()
    print("✅ Base de datos lista")

init_db()

# ========================
# HELPERS
# ========================
def get_custom_field(data, prefix, field_name):
    for i in range(20):
        if data.get(f'{prefix}[custom_fields][{i}][name]') == field_name:
            val = data.get(f'{prefix}[custom_fields][{i}][values][0]')
            try:
                return int(val)
            except (TypeError, ValueError):
                return None
    return None

CAMPOS_DEFAULT = {'cliente': 'F Ult msj cliente', 'asesor': 'F ult msj asesor'}

def campos_de(subdomain):
    """Cada cuenta de Kommo nombra distinto sus campos custom de
    'ultimo mensaje cliente/asesor' (mayusculas, typos, sufijos).
    Se configuran por subdominio en PULSE_CONFIG['campos']."""
    return PULSE_CONFIG.get(subdomain, {}).get('campos', CAMPOS_DEFAULT)

def get_batch_indices(data, key_root):
    """Kommo puede agrupar varios eventos del mismo tipo en un solo POST
    (message[add][0], message[add][1], ...). Devuelve todos los índices presentes."""
    idxs = set()
    pattern = re.compile(re.escape(key_root) + r'\[(\d+)\]')
    for k in data.keys():
        m = pattern.match(k)
        if m:
            idxs.add(int(m.group(1)))
    return sorted(idxs)

# ========================
# GUARDAR EVENTO
# ========================
def guardar_evento(subdomain, tipo_evento, lead_id, timestamp, data):
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
            json.dumps(data),
            datetime.now().isoformat()
        ))
        conn.commit()
        print(f"💾 {subdomain} | {tipo_evento} | lead: {lead_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

def nombre_asesor(responsible_user_id):
    if responsible_user_id in (None, '', 0, '0'):
        return None
    return ASESORES.get(int(responsible_user_id), f'Usuario {responsible_user_id}')

def guardar_tiempo_respuesta(subdomain, lead_id, f_cliente, f_asesor, responsible_user_id=None, lead_nombre=None):
    tiempo_seg = f_asesor - f_cliente
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO tiempos_respuesta
                (subdomain, lead_id, f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, capturado_at, responsible_user_id, lead_nombre)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (subdomain, lead_id, f_ult_msj_asesor)
            DO UPDATE SET responsible_user_id = EXCLUDED.responsible_user_id, lead_nombre = EXCLUDED.lead_nombre
        ''', (subdomain, lead_id, f_cliente, f_asesor, tiempo_seg, datetime.now().isoformat(), responsible_user_id, lead_nombre))
        conn.commit()
        print(f"⏱️  {subdomain} | lead {lead_id} | asesor respondió en {tiempo_seg // 60}min {tiempo_seg % 60}s")
    except Exception as e:
        print(f"❌ Error tiempo respuesta: {e}")
    finally:
        conn.close()

def guardar_lead_estado(subdomain, lead_id, responsible_user_id, f_cliente, f_asesor, evento_ts, lead_nombre=None):
    """Guarda el estado vigente del lead (quién es responsable, cuándo escribió
    cliente/asesor por última vez). Se llama en CADA leads[update], sin importar
    quién respondió último. evento_ts evita que un evento viejo (ej. reprocesado
    por /backfill) pise un estado más reciente."""
    if not evento_ts:
        return
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO leads_estado
                (subdomain, lead_id, responsible_user_id, f_ult_msj_cliente, f_ult_msj_asesor, evento_ts, actualizado_at, lead_nombre)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (subdomain, lead_id) DO UPDATE SET
                responsible_user_id = EXCLUDED.responsible_user_id,
                f_ult_msj_cliente   = EXCLUDED.f_ult_msj_cliente,
                f_ult_msj_asesor    = EXCLUDED.f_ult_msj_asesor,
                evento_ts           = EXCLUDED.evento_ts,
                actualizado_at      = EXCLUDED.actualizado_at,
                lead_nombre         = EXCLUDED.lead_nombre
            WHERE EXCLUDED.evento_ts >= leads_estado.evento_ts
        ''', (subdomain, lead_id, responsible_user_id, f_cliente, f_asesor, int(evento_ts), datetime.now().isoformat(), lead_nombre))
        conn.commit()
    except Exception as e:
        print(f"❌ Error lead_estado: {e}")
    finally:
        conn.close()

# ========================
# WEBHOOK
# ========================
@app.route('/webhook', methods=['POST'])
def webhook():
    # Kommo desactiva el webhook si la respuesta tarda o falla. Por eso
    # confirmamos de inmediato (200) y el guardado en BD corre en background,
    # así la lentitud de la base de datos nunca hace que Kommo lo desactive.
    data = request.form.to_dict()
    threading.Thread(target=_procesar_webhook, args=(data,), daemon=True).start()
    return jsonify({'status': 'ok'}), 200

def _procesar_webhook(data):
    try:
        subdomain = data.get('account[subdomain]', 'desconocido')
        algo_procesado = False

        for i in get_batch_indices(data, 'message[add]'):
            algo_procesado = True
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='mensaje',
                lead_id=data.get(f'message[add][{i}][element_id]'),
                timestamp=data.get(f'message[add][{i}][created_at]'),
                data=data
            )

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

            # Estado vigente del lead (para "no respondidos" / "trabajados hoy"),
            # se guarda siempre, sin importar quién escribió último.
            guardar_lead_estado(subdomain, lead_id, responsible_user_id, f_cliente, f_asesor, evento_ts, lead_nombre)

            if f_cliente and f_asesor and f_asesor > f_cliente:
                guardar_tiempo_respuesta(subdomain, lead_id, f_cliente, f_asesor, responsible_user_id, lead_nombre)

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
            print(f"⚠️  Ignorado | {subdomain} | {list(data.keys())[:2]}")

    except Exception as e:
        print(f"❌ ERROR procesando webhook en background: {e}")

# ========================
# ENDPOINTS
# ========================
@app.route('/')
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
            r['asesor'] = nombre_asesor(r.get('responsible_user_id'))

        return jsonify({'resumen': resumen, 'detalle': detalle})
    finally:
        conn.close()

@app.route('/backfill')
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
                prefix    = 'leads[update][0]'
                campos    = campos_de(row['subdomain'])
                f_cliente = get_custom_field(data, prefix, campos['cliente'])
                f_asesor  = get_custom_field(data, prefix, campos['asesor'])
                responsible_user_id = data.get(f'{prefix}[responsible_user_id]')
                evento_ts = data.get(f'{prefix}[updated_at]')
                lead_nombre = data.get(f'{prefix}[name]')

                if evento_ts:
                    write_c.execute('''
                        INSERT INTO leads_estado
                            (subdomain, lead_id, responsible_user_id, f_ult_msj_cliente, f_ult_msj_asesor, evento_ts, actualizado_at, lead_nombre)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (subdomain, lead_id) DO UPDATE SET
                            responsible_user_id = EXCLUDED.responsible_user_id,
                            f_ult_msj_cliente   = EXCLUDED.f_ult_msj_cliente,
                            f_ult_msj_asesor    = EXCLUDED.f_ult_msj_asesor,
                            evento_ts           = EXCLUDED.evento_ts,
                            actualizado_at      = EXCLUDED.actualizado_at,
                            lead_nombre         = EXCLUDED.lead_nombre
                        WHERE EXCLUDED.evento_ts >= leads_estado.evento_ts
                    ''', (
                        row['subdomain'], row['lead_id'], responsible_user_id,
                        f_cliente, f_asesor, int(evento_ts), datetime.now().isoformat(), lead_nombre
                    ))

                if f_cliente and f_asesor and f_asesor > f_cliente:
                    write_c.execute('''
                        INSERT INTO tiempos_respuesta
                            (subdomain, lead_id, f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, capturado_at, responsible_user_id, lead_nombre)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (subdomain, lead_id, f_ult_msj_asesor)
                        DO UPDATE SET responsible_user_id = EXCLUDED.responsible_user_id, lead_nombre = EXCLUDED.lead_nombre
                    ''', (
                        row['subdomain'], row['lead_id'],
                        f_cliente, f_asesor, f_asesor - f_cliente,
                        datetime.now().isoformat(), responsible_user_id, lead_nombre
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
            'siguiente': f'/backfill?offset={offset + limit}' if hay_mas else None,
            'listo': not hay_mas
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

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
                if not ok_cliente and get_custom_field(data, 'leads[update][0]', campos['cliente']) is not None:
                    ok_cliente = True
                if not ok_asesor and get_custom_field(data, 'leads[update][0]', campos['asesor']) is not None:
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

def _get_franja_idx(seg, franjas):
    for i, f in enumerate(franjas):
        if f['max_seg'] is None or seg < f['max_seg']:
            return i
    return len(franjas) - 1

def _calc_metricas(registros, franjas, tz_offset):
    if not registros:
        return {
            'total': 0,
            'promedio_seg': 0,
            'maximo_seg': 0,
            'distribucion': [
                {'label': f['label'], 'tag': f['tag'], 'count': 0, 'pct': 0.0, 'color': f['color'], 'leads': []}
                for f in franjas
            ]
        }
    total = len(registros)
    promedio_seg = round(sum(r['efectivo_seg'] for r in registros) / total)
    maximo_seg = max(r['efectivo_seg'] for r in registros)
    groups = [[] for _ in franjas]
    for r in registros:
        groups[_get_franja_idx(r['efectivo_seg'], franjas)].append(r)
    distribucion = []
    for i, f in enumerate(franjas):
        leads = []
        for r in sorted(groups[i], key=lambda x: x['efectivo_seg'], reverse=True)[:50]:
            local_dt = _ts_to_local(r['f_ult_msj_cliente'], tz_offset)
            leads.append({
                'lead_id': r['lead_id'],
                'nombre': r.get('lead_nombre'),
                'fmt': _fmt_seg(r['efectivo_seg']),
                'fecha': local_dt.strftime('%d/%m %H:%M'),
                'asesor': nombre_asesor(r.get('responsible_user_id')),
            })
        distribucion.append({
            'label': f['label'], 'tag': f['tag'],
            'count': len(groups[i]),
            'pct': round(len(groups[i]) / total * 100, 1),
            'color': f['color'],
            'leads': leads,
        })
    return {'total': total, 'promedio_seg': promedio_seg, 'maximo_seg': maximo_seg, 'distribucion': distribucion}

def _calc_por_asesor(registros):
    grupos = defaultdict(list)
    for r in registros:
        nombre = nombre_asesor(r.get('responsible_user_id')) or 'Sin asignar'
        grupos[nombre].append(r['efectivo_seg'])
    resultado = [
        {
            'asesor': nombre,
            'total': len(segs),
            'promedio_seg': round(sum(segs) / len(segs)),
        }
        for nombre, segs in grupos.items()
    ]
    resultado.sort(key=lambda x: x['promedio_seg'])
    return resultado

def _fmt_row(r, tz_offset):
    local_dt = _ts_to_local(r['f_ult_msj_cliente'], tz_offset)
    return {
        'lead_id': r['lead_id'],
        'nombre': r.get('lead_nombre'),
        'seg': r['efectivo_seg'],
        'fmt': _fmt_seg(r['efectivo_seg']),
        'fecha': local_dt.strftime('%d/%m %H:%M'),
        'asesor': nombre_asesor(r.get('responsible_user_id')),
    }

def _hoy_rango_ts(tz_offset):
    hoy_local = datetime.utcnow() + timedelta(hours=tz_offset)
    inicio = _local_date_to_ts(hoy_local.strftime('%Y-%m-%d'), tz_offset)
    return inicio, inicio + 86400

def _fmt_lead_estado(row, tz_offset, campo_fecha):
    local_dt = _ts_to_local(row[campo_fecha], tz_offset)
    return {
        'lead_id': row['lead_id'],
        'nombre':  row.get('lead_nombre'),
        'asesor':  nombre_asesor(row.get('responsible_user_id')),
        'fecha':   local_dt.strftime('%d/%m %H:%M'),
    }

def _leads_no_respondidos(subdomain, tz_offset, responsible_user_id=None):
    """Leads cuyo último mensaje en la conversación es del cliente y el
    asesor no ha respondido después. No depende de rango de fechas: es
    el estado pendiente ahora mismo."""
    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        query = '''
            SELECT lead_id, responsible_user_id, f_ult_msj_cliente, lead_nombre
            FROM leads_estado
            WHERE subdomain = %s
              AND f_ult_msj_cliente IS NOT NULL
              AND (f_ult_msj_asesor IS NULL OR f_ult_msj_cliente > f_ult_msj_asesor)
        '''
        params = [subdomain]
        if responsible_user_id:
            query += ' AND responsible_user_id = %s'
            params.append(responsible_user_id)
        query += ' ORDER BY f_ult_msj_cliente DESC'
        c.execute(query, params)
        rows = c.fetchall()
    finally:
        conn.close()
    return [_fmt_lead_estado(r, tz_offset, 'f_ult_msj_cliente') for r in rows]

def _leads_trabajados_hoy(subdomain, tz_offset, h_ini, h_fin, responsible_user_id=None):
    """Leads a los que el asesor le escribió (respuesta o mensaje propio)
    hoy, dentro del horario laboral configurado."""
    inicio, fin = _hoy_rango_ts(tz_offset)
    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        query = '''
            SELECT lead_id, responsible_user_id, f_ult_msj_asesor, lead_nombre
            FROM leads_estado
            WHERE subdomain = %s
              AND f_ult_msj_asesor >= %s AND f_ult_msj_asesor < %s
        '''
        params = [subdomain, inicio, fin]
        if responsible_user_id:
            query += ' AND responsible_user_id = %s'
            params.append(responsible_user_id)
        query += ' ORDER BY f_ult_msj_asesor DESC'
        c.execute(query, params)
        rows = c.fetchall()
    finally:
        conn.close()
    resultado = []
    for r in rows:
        local_dt = _ts_to_local(r['f_ult_msj_asesor'], tz_offset)
        if h_ini <= local_dt.hour < h_fin:
            resultado.append(_fmt_lead_estado(r, tz_offset, 'f_ult_msj_asesor'))
    return resultado

def _lista_asesores(subdomain):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            'SELECT DISTINCT responsible_user_id FROM leads_estado WHERE subdomain = %s AND responsible_user_id IS NOT NULL',
            (subdomain,)
        )
        ids = [row[0] for row in c.fetchall()]
    finally:
        conn.close()
    asesores = [{'id': uid, 'nombre': nombre_asesor(uid)} for uid in ids]
    asesores = [a for a in asesores if a['nombre']]
    return sorted(asesores, key=lambda x: x['nombre'])

def _fecha_minima(subdomain):
    """Fecha en que EMPEZAMOS A CAPTURAR datos de este cliente (cuando se
    conecto el webhook). Ojo: no usar el timestamp del evento (el
    'updated_at' que manda Kommo) porque leads viejos y dormidos pueden
    traer fechas de hace anios en el snapshot inicial al conectar - lo
    que importa es capturado_at, cuando NOSOTROS lo recibimos."""
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            'SELECT MIN(capturado_at) FROM eventos WHERE subdomain = %s',
            (subdomain,)
        )
        min_capturado = c.fetchone()[0]
    finally:
        conn.close()
    if not min_capturado:
        return None
    return min_capturado[:10]

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

    return render_template('pulse.html', subdomain=subdomain)

@app.route('/pulse/logout')
def pulse_logout():
    subdomain = request.args.get('subdomain', '').strip()
    session.pop(f'pulse_ok_{subdomain}', None)
    return redirect(url_for('pulse', subdomain=subdomain))

@app.route('/pulse/data')
def pulse_data():
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain or subdomain not in PULSE_CONFIG:
        return jsonify({'error': f'Subdomain "{subdomain}" no configurado en PULSE'}), 400
    if not pulse_autorizado(subdomain):
        return jsonify({'error': 'No autorizado'}), 401

    cfg       = PULSE_CONFIG[subdomain]
    tz_offset = cfg['tz_offset']
    h_ini, h_fin = cfg['horario']
    dias_lab  = set(cfg['dias_laborables'])
    franjas   = cfg['franjas']

    desde_str = request.args.get('desde')
    hasta_str = request.args.get('hasta')
    asesor_id = request.args.get('asesor', '').strip() or None
    fuera_horario = request.args.get('modo', 'laboral').strip() == 'fuera_horario'

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        # DISTINCT ON: si el asesor manda varios mensajes seguidos respondiendo
        # al MISMO mensaje del cliente, cada uno actualiza f_ult_msj_asesor y
        # generaba una fila "respuesta" separada, inflando el conteo. Nos
        # quedamos solo con la primera respuesta (f_ult_msj_asesor mas chico)
        # por cada mensaje distinto del cliente.
        query = '''
            SELECT DISTINCT ON (lead_id, f_ult_msj_cliente)
                lead_id, f_ult_msj_cliente, f_ult_msj_asesor, responsible_user_id, lead_nombre
            FROM tiempos_respuesta
            WHERE subdomain = %s
              AND f_ult_msj_cliente IS NOT NULL
              AND f_ult_msj_asesor  IS NOT NULL
              AND f_ult_msj_asesor > f_ult_msj_cliente
              AND (f_ult_msj_asesor - f_ult_msj_cliente) <= %s
        '''
        params = [subdomain, GAP_REACTIVACION_SEG]
        if desde_str:
            query += ' AND f_ult_msj_cliente >= %s'
            params.append(_local_date_to_ts(desde_str, tz_offset))
        if hasta_str:
            query += ' AND f_ult_msj_cliente <= %s'
            params.append(_local_date_to_ts(hasta_str, tz_offset) + 86399)
        if asesor_id:
            query += ' AND responsible_user_id = %s'
            params.append(asesor_id)
        query += ' ORDER BY lead_id, f_ult_msj_cliente, f_ult_msj_asesor ASC'
        c.execute(query, params)
        rows = c.fetchall()
    except Exception as e:
        print(f'❌ PULSE /data query: {e}')
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

    try:
        registros = []
        for row in rows:
            dt_cliente = _ts_to_local(row['f_ult_msj_cliente'], tz_offset)
            en_horario = _en_horario_laboral(dt_cliente, h_ini, h_fin, dias_lab)

            if fuera_horario:
                # Solo mensajes que llegaron FUERA del horario configurado,
                # medidos en tiempo real de reloj (sin saltar horas no
                # laborables, al reves del calculo de "tiempo efectivo").
                if en_horario:
                    continue
                efectivo = row['f_ult_msj_asesor'] - row['f_ult_msj_cliente']
            else:
                efectivo = _calc_tiempo_efectivo(
                    row['f_ult_msj_cliente'], row['f_ult_msj_asesor'],
                    tz_offset, h_ini, h_fin, dias_lab
                )

            registros.append({
                'lead_id':          row['lead_id'],
                'lead_nombre':      row['lead_nombre'],
                'f_ult_msj_cliente': row['f_ult_msj_cliente'],
                'efectivo_seg':      efectivo,
                'responsible_user_id': row['responsible_user_id'],
            })

        top_lentos  = sorted(registros, key=lambda r: r['efectivo_seg'], reverse=True)[:10]
        top_rapidos = sorted(registros, key=lambda r: r['efectivo_seg'])[:10]

        daily = defaultdict(list)
        daily_asesor = defaultdict(lambda: defaultdict(int))
        for r in registros:
            dia = _ts_to_local(r['f_ult_msj_cliente'], tz_offset).strftime('%Y-%m-%d')
            daily[dia].append(r['efectivo_seg'])
            if not asesor_id:
                nombre = nombre_asesor(r.get('responsible_user_id')) or 'Sin asignar'
                daily_asesor[dia][nombre] += 1

        dias_ordenados = sorted(daily.items())[-30:]
        tendencia = [
            {'fecha': dia, 'promedio_h': round(sum(v) / len(v) / 3600, 1), 'total': len(v)}
            for dia, v in dias_ordenados
        ]

        tendencia_por_asesor = None
        if not asesor_id:
            # Barras apiladas por asesor: solo tiene sentido en la vista "Todos"
            # (con un asesor ya filtrado la barra seria una sola serie).
            dias = [dia for dia, _ in dias_ordenados]
            nombres = sorted({n for dia in dias for n in daily_asesor[dia]})
            PALETA_ASESORES = ['#2563eb', '#16a34a', '#f59e0b', '#0891b2', '#e11d48', '#65a30d', '#0d9488', '#0284c7']
            tendencia_por_asesor = [
                {
                    'asesor': nombre,
                    'color': PALETA_ASESORES[i % len(PALETA_ASESORES)],
                    'data': [daily_asesor[dia].get(nombre, 0) for dia in dias],
                }
                for i, nombre in enumerate(nombres)
            ]

        no_respondidos  = _leads_no_respondidos(subdomain, tz_offset, asesor_id)
        trabajados_hoy  = _leads_trabajados_hoy(subdomain, tz_offset, h_ini, h_fin, asesor_id)

        return jsonify({
            'config': {
                'nombre':      cfg['nombre'],
                'horario_fmt': _fmt_horario(h_ini, h_fin),
                'dias_fmt':    ' · '.join(DIAS_LABEL[d] for d in sorted(dias_lab)),
                'franjas':     franjas,
                'crm_url':     cfg.get('crm_domain') and f'https://{cfg["crm_domain"]}',
                'asesores':    _lista_asesores(subdomain),
                'asesor_actual': nombre_asesor(asesor_id) if asesor_id else None,
                'fecha_minima': _fecha_minima(subdomain),
                'modo':        'fuera_horario' if fuera_horario else 'laboral',
            },
            'metricas':      _calc_metricas(registros, franjas, tz_offset),
            'top_lentos':    [_fmt_row(r, tz_offset) for r in top_lentos],
            'top_rapidos':   [_fmt_row(r, tz_offset) for r in top_rapidos],
            'tendencia':     tendencia,
            'tendencia_por_asesor': tendencia_por_asesor,
            'por_asesor':    _calc_por_asesor(registros) if not asesor_id else None,
            'no_respondidos': no_respondidos,
            'trabajados_hoy': trabajados_hoy,
        })
    except Exception as e:
        print(f'❌ PULSE /data procesamiento: {e}')
        return jsonify({'error': str(e)}), 500

# ========================
# MAIN
# ========================
if __name__ == '__main__':
    print("🚀 Servidor corriendo en puerto 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)

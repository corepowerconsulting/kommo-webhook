"""Leads eliminados y fusionados en Kommo, contra PostgreSQL REAL (27/10).

Usa la base de prueba de .pruebas/pgenv (pgserver) y un Kommo simulado que
devuelve eventos armados aca. Comprueba que:
  - un lead eliminado sale de Sin responder y de "mas de 7 dias";
  - una fusion pasa los mensajes al lead que quedo, sin choques de clave, y la
    respuesta dada en el lead nuevo cierra la espera del viejo;
  - las cadenas (A en B, B en C) terminan en C;
  - una fusion dudosa se trata como eliminado, sin mover nada;
  - un lead restaurado vuelve; correrlo dos veces no duplica nada;
  - un par eliminado/fusion partido entre dos ventanas se empareja igual.

Uso:  env\\Scripts\\python.exe scripts\\prueba_eliminados.py
"""
import io, os, sys, time, contextlib
from datetime import datetime, timedelta
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, '.pruebas', 'pgenv'))
import pgserver, psycopg2
URI = pgserver.get_server(os.path.join(RAIZ, '.pruebas', 'pgdata'), cleanup_mode='stop').get_uri()
c0 = psycopg2.connect(URI); c0.autocommit = True
c0.cursor().execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;'); c0.close()
_c = psycopg2.connect
psycopg2.connect = lambda d, **k: _c(d, **{a: b for a, b in k.items() if a != 'sslmode'})
os.environ.update({'DATABASE_URL': URI, 'ADMIN_TOKEN': 'x', 'PULSE_SIN_HILOS': '1'})
sys.path.insert(0, RAIZ)
with contextlib.redirect_stdout(io.StringIO()):
    import kommo

fallos = 0
def check(n, c, d=''):
    global fallos
    print(('OK   ' if c else 'FALLA'), n, '' if c else d); fallos += 0 if c else 1

S = 'gruporegalado'
tz = kommo.PULSE_CONFIG[S]['tz_offset']
ahora = int(time.time()); D = 86400
uid, nombre = list(kommo.ASESORES.items())[0]
conn = _c(URI); cur = conn.cursor()
n = [0]
def cliente(lead, ts):
    cur.execute('INSERT INTO mensajes_cliente (subdomain, lead_id, ts) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING', (S, lead, ts))
def asesor(lead, ts):
    n[0] += 1
    cur.execute('INSERT INTO mensajes_asesor (subdomain, msg_id, lead_id, ts, user_id, autor_nombre) VALUES (%s,%s,%s,%s,%s,%s)', (S, f'm{n[0]}', lead, ts, uid, nombre))
def estado(lead, ts):
    cur.execute('INSERT INTO leads_estado (subdomain, lead_id, responsible_user_id, f_ult_msj_cliente, evento_ts, pipeline_id, status_id) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                (S, lead, uid, ts, ts, 1, 1))
    cur.execute('INSERT INTO leads_cambios (subdomain, lead_id, desde_ts, responsible_user_id, asesor_nombre, pipeline_id, status_id) VALUES (%s,%s,%s,%s,NULL,%s,%s) ON CONFLICT DO NOTHING',
                (S, lead, ts - 60, uid, 1, 1))

# Un saliente viejo en otro lead: el corte de la captura.
asesor('VIEJISIMO', ahora - 40 * D); cliente('VIEJISIMO', ahora - 40 * D - 60); estado('VIEJISIMO', ahora - 40 * D - 60)
# Eliminado viejo (aparece en "mas de 7 dias") y eliminado reciente (en Sin responder).
cliente('BORRADO_VIEJO', ahora - 10 * D); estado('BORRADO_VIEJO', ahora - 10 * D)
cliente('BORRADO_HOY', ahora - 3600); estado('BORRADO_HOY', ahora - 3600)
# Fusion: el cliente escribe en VIEJO; despues de fusionar le contestan en NUEVO.
t0 = ahora - 2 * D
cliente('VIEJO', t0); cliente('VIEJO', t0 + 30); estado('VIEJO', t0 + 30)
cliente('NUEVO', t0 + 30)                     # mismo segundo en los dos: no puede chocar la clave
asesor('VIEJO', t0 - 600)                     # un saliente viejo del lead absorbido
estado('NUEVO', t0 + 30); asesor('NUEVO', t0 + 3600)
# Cadena: A se fusiona en B, B en C.
cliente('A', ahora - 3 * D); estado('A', ahora - 3 * D)
cliente('B', ahora - 3 * D + 100); estado('B', ahora - 3 * D + 100)
cliente('C', ahora - 3 * D + 200); estado('C', ahora - 3 * D + 200); asesor('C', ahora - D)
# Dudosa: dos fusiones del mismo usuario en el mismo segundo.
cliente('DUDOSO', ahora - 4 * D); estado('DUDOSO', ahora - 4 * D)
# Restaurado: se elimina y se restaura.
cliente('RESTAURADO', ahora - 5 * D); estado('RESTAURADO', ahora - 5 * D)
# Par partido entre ventanas: el eliminado 2 s antes del borde, la fusion 3 s despues.
cliente('BORDE', ahora - 6 * D); estado('BORDE', ahora - 6 * D); cliente('BORDE_DESTINO', ahora - 6 * D + 5); estado('BORDE_DESTINO', ahora - 6 * D + 5)
conn.commit()

# --- Kommo simulado ---------------------------------------------------------
EV = []
def evento(tipo, lead, ts, por=111):
    EV.append({'type': tipo, 'entity_type': 'lead', 'entity_id': lead, 'created_at': ts, 'created_by': por})
evento('lead_deleted', 'BORRADO_VIEJO', ahora - 9 * D)
evento('lead_deleted', 'BORRADO_HOY', ahora - 1800)
evento('lead_deleted', 'VIEJO', t0 + 600); evento('entity_merged', 'NUEVO', t0 + 600)
evento('lead_deleted', 'A', ahora - 3 * D + 1000); evento('entity_merged', 'B', ahora - 3 * D + 1002)
evento('lead_deleted', 'B', ahora - 3 * D + 5000); evento('entity_merged', 'C', ahora - 3 * D + 5001)
evento('lead_deleted', 'DUDOSO', ahora - 4 * D + 500, por=222)
evento('entity_merged', 'OTRO1', ahora - 4 * D + 500, por=222); evento('entity_merged', 'OTRO2', ahora - 4 * D + 501, por=222)
evento('lead_deleted', 'RESTAURADO', ahora - 5 * D + 100); evento('lead_restored', 'RESTAURADO', ahora - 5 * D + 900)
BORDE_T = ahora - 6 * D + 3600 * 5          # se alinea con una ventana en la prueba de abajo
evento('lead_deleted', 'BORDE', BORDE_T - 2, por=333); evento('entity_merged', 'BORDE_DESTINO', BORDE_T + 3, por=333)

def kommo_falso(subdomain, token, ruta):
    from urllib.parse import urlparse, parse_qs
    q = parse_qs(urlparse(ruta).query)
    tipo = q['filter[type]'][0]; desde = int(q['filter[created_at][from]'][0]); hasta = int(q['filter[created_at][to]'][0])
    pagina = int(q.get('page', ['1'])[0])
    sel = [e for e in EV if e['type'] == tipo and desde <= e['created_at'] <= hasta]
    trozo = sel[(pagina - 1) * 100: pagina * 100]
    return {'_embedded': {'events': trozo}} if trozo else None
kommo._kommo_get = kommo_falso

cl = kommo.app.test_client()
with cl.session_transaction() as s_:
    s_['pulse_ok_' + S] = True
def listas():
    with contextlib.redirect_stdout(io.StringIO()):
        d = cl.get(f'/pulse/snapshot?subdomain={S}&incluir_cerradas=1').get_json()
    return {l['lead_id'] for l in d.get('no_respondidos', [])}, {l['lead_id'] for l in d.get('no_respondidos_antiguos', [])}

recientes, antiguos = listas()
check('ANTES: BORRADO_HOY y VIEJO estan en Sin responder', {'BORRADO_HOY', 'VIEJO'} <= recientes, recientes)
check('ANTES: BORRADO_VIEJO esta en "mas de 7 dias"', 'BORRADO_VIEJO' in antiguos, antiguos)

# Ventanas chicas para probar el borde: el par BORDE cae partido.
kommo.ELIMINADOS_VENTANA_SEG = 3600 * 5
cur.execute("INSERT INTO kommo_sync (subdomain, clave, valor) VALUES (%s, 'eliminados_hasta', %s)", (S, BORDE_T - 3600 * 5 * 30)); conn.commit()
kommo.ELIMINADOS_SEG_POR_VUELTA = 999
with contextlib.redirect_stdout(io.StringIO()):
    r1 = kommo._sincronizar_eliminados(S, 'tok', ahora)
# Desde el inicio de los eventos: una segunda pasada que arranque bien atras para los demas.
cur.execute("UPDATE kommo_sync SET valor = %s WHERE subdomain = %s", (ahora - 11 * D, S)); conn.commit()
kommo.ELIMINADOS_VENTANA_SEG = 7 * D
with contextlib.redirect_stdout(io.StringIO()):
    r2 = kommo._sincronizar_eliminados(S, 'tok', ahora)
print('   resumen:', r1, r2)

cur.execute('SELECT lead_id, fusionado_en FROM leads_eliminados WHERE subdomain = %s', (S,))
tabla = dict(cur.fetchall())
check('BORDE se emparejo aunque el par cae entre dos ventanas', tabla.get('BORDE') == 'BORDE_DESTINO', tabla)
check('VIEJO quedo como fusionado en NUEVO', tabla.get('VIEJO') == 'NUEVO', tabla)
check('DUDOSO quedo como eliminado sin fusion', 'DUDOSO' in tabla and tabla['DUDOSO'] is None, tabla)
check('RESTAURADO no esta como eliminado', 'RESTAURADO' not in tabla, tabla)
cur.execute("SELECT lead_id, COUNT(*) FROM mensajes_cliente WHERE subdomain = %s GROUP BY lead_id", (S,))
cli = dict(cur.fetchall())
check('los mensajes de VIEJO pasaron a NUEVO, sin duplicar el del mismo segundo', 'VIEJO' not in cli and cli.get('NUEVO') == 2, cli)
check('cadena: A y B terminaron en C', 'A' not in cli and 'B' not in cli and cli.get('C') == 3, cli)
check('DUDOSO no movio nada', cli.get('DUDOSO') == 1, cli)
cur.execute("SELECT COUNT(*) FROM mensajes_asesor WHERE subdomain = %s AND lead_id = 'VIEJO'", (S,))
check('los salientes de VIEJO tambien pasaron', cur.fetchone()[0] == 0)
cur.execute("SELECT lead_id FROM leads_estado WHERE subdomain = %s", (S,))
estados = {r[0] for r in cur.fetchall()}
check('las filas de estado de los absorbidos se borraron', not ({'VIEJO', 'A', 'B', 'BORDE'} & estados) and {'NUEVO', 'C'} <= estados, estados)

recientes, antiguos = listas()
check('DESPUES: BORRADO_HOY ya no esta en Sin responder', 'BORRADO_HOY' not in recientes, recientes)
check('DESPUES: BORRADO_VIEJO ya no esta en "mas de 7 dias"', 'BORRADO_VIEJO' not in antiguos, antiguos)
check('DESPUES: VIEJO ya no esta, y NUEVO no aparece porque le contestaron', 'VIEJO' not in recientes and 'NUEVO' not in recientes, recientes)
check('RESTAURADO sigue en las listas', 'RESTAURADO' in recientes | antiguos, (recientes, antiguos))
kommo._turnos_cache.clear()
c = conn.cursor(cursor_factory=__import__('psycopg2.extras', fromlist=['RealDictCursor']).RealDictCursor)
turnos = kommo._turnos_cacheados(c, S, kommo._corte_salientes(c, S), None)
t_nuevo = [t for t in turnos if t['lead_id'] == 'NUEVO']
check('la espera del cliente de VIEJO ahora se mide con la respuesta en NUEVO (1 h)', len(t_nuevo) == 1 and t_nuevo[0]['inicio'] == t0 and t_nuevo[0]['fin'] == t0 + 3600, t_nuevo)

antes = dict(tabla)
with contextlib.redirect_stdout(io.StringIO()):
    cur.execute("UPDATE kommo_sync SET valor = %s WHERE subdomain = %s", (ahora - 11 * D, S)); conn.commit()
    r3 = kommo._sincronizar_eliminados(S, 'tok', ahora)
cur.execute('SELECT lead_id, fusionado_en FROM leads_eliminados WHERE subdomain = %s', (S,))
check('correrlo de nuevo no cambia nada ni rehace trabajo', dict(cur.fetchall()) == antes and r3['eliminados'] == 0 and r3['fusionados'] == 0 and r3['restaurados'] == 0, r3)
cur.execute("SELECT valor FROM kommo_sync WHERE subdomain = %s AND clave = 'eliminados_hasta'", (S,))
check('el punto de avance llega hasta hace un minuto', cur.fetchone()[0] == ahora - 60)

# Un Kommo que falla no rompe nada y no avanza el punto.
cur.execute("UPDATE kommo_sync SET valor = %s WHERE subdomain = %s", (ahora - D, S)); conn.commit()
kommo._kommo_get = lambda *a: (_ for _ in ()).throw(OSError('Kommo no responde'))
try:
    kommo._sincronizar_eliminados(S, 'tok', ahora); check('si Kommo falla, avisa con un error', False)
except OSError:
    cur.execute("SELECT valor FROM kommo_sync WHERE subdomain = %s AND clave = 'eliminados_hasta'", (S,))
    check('si Kommo falla, no avanza el punto (se reintenta en la proxima vuelta)', cur.fetchone()[0] == ahora - D)
print('\nFALLOS:', fallos); sys.exit(1 if fallos else 0)

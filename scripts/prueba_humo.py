"""Prueba de humo del tablero, sin base de datos (27/10).

Recorre TODAS las cuentas con muchas combinaciones de filtros —tambien valores
mal escritos— contra /pulse/data y /pulse/snapshot, dos veces (la segunda pasa
por los caches), con una base simulada que no devuelve filas. Lo que exige: que
siempre responda JSON, sin pagina de error. Es la clase de falla del 27/10 (un
filtro de embudo rompia la clave del cache), que no depende de los datos.

Tambien prueba el freno de memoria, el limite de los caches y que un error no
previsto salga como JSON.

Uso:  env\\Scripts\\python.exe scripts\\prueba_humo.py
"""
import io, os, sys, contextlib, itertools, random
from datetime import datetime, timedelta

AQUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(AQUI))
import psycopg2

class _Cursor:
    rowcount = 0
    def __getattr__(self, n): return lambda *a, **k: None
    def fetchall(self): return []
    def fetchone(self): return None
    def fetchmany(self, *a): return []
    def __iter__(self): return iter([])
class _Conexion:
    autocommit = True
    def cursor(self, *a, **k): return _Cursor()
    def __getattr__(self, n): return lambda *a, **k: None
psycopg2.connect = lambda *a, **k: _Conexion()
os.environ.update({'DATABASE_URL': 'postgresql://x@127.0.0.1:1/x', 'ADMIN_TOKEN': 'x', 'PULSE_SIN_HILOS': '1', 'SECRET_KEY': 'prueba'})
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    import kommo

fallos = []
def check(nombre, ok, detalle=''):
    if not ok:
        fallos.append(f'{nombre}  {detalle}')

hoy = datetime.utcnow()
f = lambda d: (hoy - timedelta(days=d)).strftime('%Y-%m-%d')
SUELTOS = [
    '', f'desde={f(29)}&hasta={f(0)}', f'desde={f(6)}&hasta={f(0)}', f'desde={f(0)}&hasta={f(0)}', f'desde={f(90)}&hasta={f(60)}',
    'modo=fuera_horario', 'modo=laboral', 'embudo=8969355', 'embudo=1&embudo=2', 'etapa=8969355:142', 'etapa=1:2&etapa=3:4',
    'asesor=6453065', 'asesor=1&asesor=2', 'asesor_nombre=Daniela', 'tj_asesor=6453065', 'tj_embudo=8969355',
    'tj_etapa=8969355:142', 'tj_asesorcampo=Daniela||Marco', f'dia={f(1)}', f'comparar={f(2)}', f'dia={f(3)}&comparar={f(7)}',
    'incluir_cerradas=1', 'perfil=1',
    # mal escritos: tienen que dar JSON igual
    'desde=ayer&hasta=hoy', f'desde={f(0)}&hasta={f(9)}', 'dia=abc', 'comparar=2026-13-45', 'asesor=abc', 'embudo=abc',
    'etapa=sin-dos-puntos', 'tj_asesor=xx&tj_embudo=yy', 'modo=otro',
]
TODO_JUNTO = '&'.join(x for x in SUELTOS[:23] if x and not x.startswith(('desde', 'dia', 'comparar', 'modo')))
R = random.Random(27)
PARES = ['&'.join(p) for p in R.sample(list(itertools.combinations([x for x in SUELTOS if x], 2)), 40)]
COMBOS = SUELTOS + [TODO_JUNTO] + PARES

cl = kommo.app.test_client()
pedidos = 0
for sub in kommo.PULSE_CONFIG:
    with cl.session_transaction() as s:
        s['pulse_ok_' + sub] = True
    for vuelta in (1, 2):
        for q in COMBOS:
            for ruta in ('/pulse/data', '/pulse/snapshot'):
                url = f'{ruta}?subdomain={sub}' + (f'&{q}' if q else '')
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    r = cl.get(url)
                pedidos += 1
                d = r.get_json(silent=True)
                check(f'{sub} {ruta} [{q}] vuelta {vuelta}: responde JSON', d is not None, f'{r.status_code} {r.data[:80]!r}')
                if d is not None and r.status_code >= 500:
                    check(f'{sub} {ruta} [{q}] vuelta {vuelta}: sin error 500', False, d.get('error'))

# Un error no previsto adentro del calculo sale como JSON, no como pagina.
orig = kommo._bloque_ahora
kommo._bloque_ahora = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('prueba'))
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    r = cl.get('/pulse/snapshot?subdomain=lizondrohomes')
kommo._bloque_ahora = orig
d = r.get_json(silent=True)
check('error no previsto en /pulse/snapshot sale como JSON 500', r.status_code == 500 and d and 'error' in d, (r.status_code, r.data[:80]))
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    r = cl.get('/no-existe')
check('una ruta que no existe sigue dando 404 normal', r.status_code == 404)

def _protecciones():
    """Limite de los caches y freno de memoria (desde el 27/10)."""
    c = {}
    for i in range(200):
        kommo._con_cache(c, ('k', i), 30, lambda: i)
    check('el cache no pasa del maximo de entradas', len(c) <= kommo.CACHE_MAX_ENTRADAS, len(c))
    _freno()

def _freno():
  # El freno de memoria suelta las cuentas que nadie mira y deja la pedida.
  import time as _t
  mem = kommo._memoria_mb
  kommo._turnos_cache.clear(); kommo._turnos_uso.clear()
  for s_, hace in (('a', 0), ('b', 20 * 60), ('c', 30 * 60)):
      kommo._turnos_cache[s_] = ((None,) * 4, [1], {'por_lead': {}, 'armado': 0})
      kommo._turnos_uso[s_] = _t.monotonic() - hace
  kommo._memoria_mb = lambda: {'memoria_mb': 300, 'memoria_pico_mb': 300}
  kommo._aliviar_memoria('a')
  check('debajo del limite no suelta nada', set(kommo._turnos_cache) == {'a', 'b', 'c'}, list(kommo._turnos_cache))
  kommo._memoria_mb = lambda: {'memoria_mb': kommo.MEMORIA_FRENO_MB + 30, 'memoria_pico_mb': 500}
  with contextlib.redirect_stdout(io.StringIO()):
      kommo._aliviar_memoria('a')
  check('arriba del limite suelta las ociosas y deja la pedida', set(kommo._turnos_cache) == {'a'}, list(kommo._turnos_cache))
  kommo._turnos_cache['d'] = ((None,) * 4, [1], {'por_lead': {}, 'armado': 0}); kommo._turnos_uso['d'] = _t.monotonic()
  with contextlib.redirect_stdout(io.StringIO()):
      kommo._aliviar_memoria('a')
  check('si no hay ociosas, suelta todas menos la pedida', set(kommo._turnos_cache) == {'a'}, list(kommo._turnos_cache))
  kommo._memoria_mb = mem

if hasattr(kommo, 'CACHE_MAX_ENTRADAS'):
    _protecciones()

print(f'{pedidos} pedidos en {len(kommo.PULSE_CONFIG)} cuentas y {len(COMBOS)} combinaciones de filtros')
for x in fallos[:30]:
    print('FALLA', x)
print('FALLOS:', len(fallos))
sys.exit(1 if fallos else 0)

"""Busca los mensajes SALIENTES del asesor por la API v4, sin webhooks.

Por que existe: el webhook no los manda. Ya esta medido —0 de ~5800 eventos
'message[add]' revisados en produccion traen type=outgoing, y suscribir
add_outgoing_message por API tampoco los hizo aparecer (probado en dos cuentas
con dos tipos de canal)—. Y la Chats API de Kommo, que si los entrega, exige
ser DUENO del canal por donde pasa la conversacion; el WhatsApp de los
clientes pasa por el canal de su proveedor actual, no por uno nuestro.

Queda un camino sin probar, y es el que este script prueba: que el mensaje del
asesor este guardado en el lead como NOTA. Si esta, se lee con el token que ya
tenemos, sin tocar nada del CRM del cliente y sin depender de terceros.

Este script NO escribe nada. Solo lee.

    $env:KOMMO_TOKEN = "el-token-largo"
    python scripts/buscar_salientes.py corepowerconsulting

Que mirar en la salida: la seccion 'tipos de nota'. Si aparece algun tipo con
texto de mensajes y con 'de' distinto del cliente, ese es el dato que falta.
Si todos los tipos son de sistema (lead_status_changed, common, call_in...),
entonces por la API tampoco estan y el unico camino es el canal de Fernando.
"""

import datetime
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict


def fecha(ts):
    if not ts:
        return '-'
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).strftime('%d/%m %H:%M UTC')
    except (TypeError, ValueError, OSError):
        return str(ts)


def pedir(subdomain, token, ruta, params=None):
    """GET a la API v4. Devuelve (json, error). No lanza: varias de las rutas
    que se prueban aca pueden no existir, y un 404 es un resultado valido de
    la investigacion, no una falla del script."""
    url = f'https://{subdomain}.kommo.com/api/v4/{ruta}'
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            if r.status == 204:          # la API contesta 204 cuando no hay nada
                return {}, None
            crudo = r.read().decode()
            return (json.loads(crudo) if crudo.strip() else {}), None
    except urllib.error.HTTPError as e:
        return None, f'HTTP {e.code} {e.reason}'
    except Exception as e:
        return None, str(e)


def texto_de(nota):
    """Saca algo legible de params, que cambia de forma segun el tipo de nota."""
    p = nota.get('params') or {}
    for k in ('text', 'link', 'phone', 'source', 'service'):
        v = p.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().replace('\n', ' ')[:70]
    return json.dumps(p, ensure_ascii=False)[:70] if p else ''


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    subdomain = sys.argv[1]
    token = os.environ.get('KOMMO_TOKEN', '').strip()
    if not token:
        print('Falta KOMMO_TOKEN. En PowerShell:  $env:KOMMO_TOKEN = "..."')
        return 1

    print(f'Cuenta: {subdomain}\n')

    # --- 1. Las notas mas recientes de TODA la cuenta -------------------------
    # Una sola llamada contesta la pregunta de fondo: si el mensaje del asesor
    # queda como nota, tiene que aparecer aca, porque son las ultimas 250 notas
    # de todos los leads.
    print('=' * 70)
    print('1. Ultimas notas de todos los leads')
    print('=' * 70)
    datos, err = pedir(subdomain, token, 'leads/notes', {
        'limit': 250, 'order[created_at]': 'desc',
    })
    if err:
        print(f'  no se pudo leer: {err}')
        notas = []
    else:
        notas = ((datos or {}).get('_embedded') or {}).get('notes') or []
    print(f'  notas leidas: {len(notas)}\n')

    tipos = defaultdict(lambda: {'veces': 0, 'ejemplos': []})
    for n in notas:
        t = n.get('note_type') or '(sin tipo)'
        e = tipos[t]
        e['veces'] += 1
        if len(e['ejemplos']) < 2:
            e['ejemplos'].append(n)

    if tipos:
        print('  tipos de nota (lo que hay que mirar):')
        for t, e in sorted(tipos.items(), key=lambda kv: -kv[1]['veces']):
            print(f'\n    {t}  ({e["veces"]})')
            for n in e['ejemplos']:
                quien = n.get('created_by')
                quien = 'CLIENTE (robot/externo)' if quien == 0 else f'usuario {quien}'
                print(f'      lead {n.get("entity_id")} · {fecha(n.get("created_at"))} '
                      f'· {quien}')
                txt = texto_de(n)
                if txt:
                    print(f'        "{txt}"')
    else:
        print('  ninguna nota. La cuenta puede no tener actividad reciente.')

    # created_by = 0 es el sistema o el cliente; distinto de 0 es un usuario de
    # Kommo, o sea el asesor. Si hay notas de tipo mensaje con created_by != 0,
    # ahi esta la respuesta del asesor con su hora exacta y quien la mando.
    con_usuario = sum(1 for n in notas if n.get('created_by'))
    print(f'\n  notas creadas por un usuario de Kommo: {con_usuario} de {len(notas)}')

    # --- 2. Rutas de chat que podrian existir --------------------------------
    # No estan documentadas para consumidores; se prueban una por una porque un
    # 404 se responde en un segundo y despeja la duda para siempre.
    print()
    print('=' * 70)
    print('2. Rutas de conversaciones')
    print('=' * 70)
    for ruta, params in [
        ('talks',           {'limit': 5}),
        ('chats',           {'limit': 5}),
        ('leads/chats',     {'limit': 5}),
        ('leads/unsorted',  {'limit': 1}),
    ]:
        datos, err = pedir(subdomain, token, ruta, params)
        if err:
            print(f'  /{ruta:16s} -> {err}')
            continue
        emb = ((datos or {}).get('_embedded') or {})
        resumen = ', '.join(f'{k}={len(v)}' for k, v in emb.items()
                            if isinstance(v, list)) or 'vacio'
        print(f'  /{ruta:16s} -> OK  {resumen}')
        for k, v in emb.items():
            if isinstance(v, list) and v:
                print(f'      claves de {k}[0]: {sorted(v[0].keys())}')

    print()
    print('=' * 70)
    print('Como leer esto')
    print('=' * 70)
    print('  Si en (1) hay un tipo de nota con el TEXTO de las respuestas del')
    print('  asesor y created_by != 0 -> se puede medir con la API, sin depender')
    print('  de nadie. Si solo hay notas de sistema, no estan, y el unico camino')
    print('  documentado es la Chats API, que exige ser dueno del canal.')
    return 0


if __name__ == '__main__':
    sys.exit(main())

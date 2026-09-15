# -*- coding: utf-8 -*-
"""Carga en PULSE el estado ACTUAL de las conversaciones de los leads en "Sin responder".

Por que existe: los avisos add_talk / update_talk llegan recien desde que se
suscribe el webhook (ver suscribir_conversaciones.py). Una conversacion que ya
estaba cerrada antes no vuelve a avisar, y ese lead seguiria en "Sin responder"
hasta que algo cambie. Esto las trae una vez desde la API de Kommo.

El camino, medido el 15/09/2026:
  - la API de conversaciones IGNORA filter[entity_id] (devuelve las de otros
    leads) pero SI respeta filter[contact_id];
  - asi que lead -> contactos (API de leads, with=contacts) -> conversaciones
    por contacto, quedandose con las que apuntan a ese lead.
Cada respuesta se valida: si Kommo trae algo que no se pidio, se corta en vez
de cargar datos de otro lead.

Uso:
    $env:PULSE_TOKEN = "el ADMIN_TOKEN de Render"
    python scripts/bajar_conversaciones.py gruporegalado          # solo cuenta
    python scripts/bajar_conversaciones.py todas --cargar         # y carga

Los tokens de Kommo salen de .env.kommo. Ningun token se imprime.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
PULSE = 'https://kommo-webhook-mp4u.onrender.com'
LOTE = 50


def tokens():
    out = {}
    with open(os.path.join(RAIZ, '.env.kommo'), encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith('#') and '=' in linea:
                sub, tok = linea.split('=', 1)
                out[sub.strip()] = tok.strip()
    return out


def pedir_json(url, token=None, cuerpo=None, timeout=120):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    for intento in range(6):
        req = urllib.request.Request(url, data=datos, headers=headers,
                                     method='POST' if datos else 'GET')
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                crudo = r.read()
                return json.loads(crudo) if crudo.strip() else {}
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2 + 2 * intento)
                continue
            if e.code == 204:
                return {}
            # Sin la URL: la de PULSE lleva el token en la query.
            raise SystemExit(f'HTTP {e.code} en {urllib.parse.urlsplit(url).path}')
    raise SystemExit('Kommo respondio 429 demasiadas veces')


def embebidos(d, clave=None):
    emb = d.get('_embedded') or {}
    if clave:
        return emb.get(clave) or []
    return next((v for v in emb.values() if isinstance(v, list)), [])


def leads_sin_responder(subdomain, pulse_token):
    # Sin abiertas=1: tambien los ganados/perdidos, para que sirva con la
    # casilla "incluir cerradas" marcada.
    q = urllib.parse.urlencode({'subdomain': subdomain, 'token': pulse_token})
    d = pedir_json(f'{PULSE}/pulse/data?{q}', timeout=180)
    return sorted({int(l['lead_id']) for l in d.get('no_respondidos') or []})


def contactos_de(subdomain, token, leads):
    out = {}
    for i in range(0, len(leads), LOTE):
        lote = leads[i:i + LOTE]
        q = '&'.join(f'filter[id][]={x}' for x in lote)
        d = pedir_json(f'https://{subdomain}.kommo.com/api/v4/leads?limit=250&with=contacts&{q}', token)
        for l in embebidos(d, 'leads'):
            if l['id'] not in lote:
                raise SystemExit('Kommo no respeto el filtro de leads: se corta')
            out[l['id']] = [c['id'] for c in ((l.get('_embedded') or {}).get('contacts') or [])]
        time.sleep(0.2)
    return out


def conversaciones_de_contactos(subdomain, token, lote):
    """Todas las conversaciones de estos contactos, o None si Kommo no filtro."""
    q = '&'.join(f'filter[contact_id][]={x}' for x in lote)
    pagina, todas = 1, []
    while True:
        ts = embebidos(pedir_json(
            f'https://{subdomain}.kommo.com/api/v4/talks?limit=250&page={pagina}&{q}', token))
        if any(t.get('contact_id') not in lote for t in ts):
            return None
        todas += ts
        if len(ts) < 250:
            return todas
        pagina += 1
        time.sleep(0.2)


def una_cuenta(subdomain, token, pulse_token, cargar):
    print(f'\n{subdomain}\n' + '=' * 66, flush=True)
    leads = leads_sin_responder(subdomain, pulse_token)
    print(f'  sin responder en PULSE: {len(leads)}', flush=True)
    if not leads:
        return True

    contactos = contactos_de(subdomain, token, leads)
    todos = sorted({c for cs in contactos.values() for c in cs})
    print(f'  contactos: {len(todos)} (de {len(contactos)} leads que Kommo devolvio)', flush=True)

    paso = LOTE if len(todos) < 2 or conversaciones_de_contactos(subdomain, token, todos[:2]) is not None else 1
    set_leads = set(leads)
    filas = {}
    for i in range(0, len(todos), paso):
        lote = todos[i:i + paso]
        ts = conversaciones_de_contactos(subdomain, token, lote)
        if ts is None:
            raise SystemExit(f'Kommo no respeto el filtro por contacto en {lote[0]}: se corta')
        for t in ts:
            if t.get('entity_type') != 'lead' or t.get('entity_id') not in set_leads:
                continue
            filas[t['talk_id']] = {
                'talk_id':        t['talk_id'],
                'lead_id':        t['entity_id'],
                'abierta':        bool(t.get('is_in_work')),
                'creada_ts':      t.get('created_at'),
                'actualizada_ts': t.get('updated_at'),
            }
        time.sleep(0.2)

    ultima = {}
    for f in filas.values():
        if f['lead_id'] not in ultima or f['talk_id'] > ultima[f['lead_id']]['talk_id']:
            ultima[f['lead_id']] = f
    cerradas = sum(1 for f in ultima.values() if not f['abierta'])
    print(f'  conversaciones: {len(filas)}   leads con la ultima CERRADA: {cerradas} de {len(leads)}',
          flush=True)

    if not cargar:
        print('  (no se cargo nada: repetir con --cargar)')
        return True

    guardadas = 0
    lista = list(filas.values())
    q = urllib.parse.urlencode({'token': pulse_token})
    for i in range(0, len(lista), 1000):
        r = pedir_json(f'{PULSE}/cargar-conversaciones?{q}', cuerpo={
            'subdomain': subdomain, 'conversaciones': lista[i:i + 1000]})
        if r.get('error'):
            raise SystemExit(f'PULSE rechazo la carga: {r["error"]}')
        guardadas += r.get('guardadas', 0)
    print(f'  cargadas en PULSE: {guardadas}', flush=True)
    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    pulse_token = os.environ.get('PULSE_TOKEN', '').strip()
    if not pulse_token:
        print('Falta PULSE_TOKEN (el ADMIN_TOKEN de Render).')
        sys.exit(1)
    cual, cargar = sys.argv[1], '--cargar' in sys.argv
    toks = tokens()
    cuentas = list(toks) if cual == 'todas' else [cual]
    for sub in cuentas:
        if sub not in toks:
            print(f'\n{sub}: sin token en .env.kommo')
            continue
        una_cuenta(sub, toks[sub], pulse_token, cargar)


if __name__ == '__main__':
    main()

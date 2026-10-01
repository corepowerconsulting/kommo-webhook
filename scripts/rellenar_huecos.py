# -*- coding: utf-8 -*-
"""Rellena huecos de captura con el registro de eventos de Kommo.

Por que existe: hubo ventanas en que PULSE no guardo nada —el 31/08-02/09 la
base estuvo 43 horas en solo lectura; el 28/08 en Camara China; el 09/09-14/09
Kommo apago el webhook de Ventas Directas—. Los leads respondidos en esas
ventanas siguen figurando "sin responder" porque la respuesta nunca se guardo.
El registro de eventos de Kommo (GET /api/v4/events) tiene cada mensaje de
chat con hora, lead, id y usuario, y llega por lo menos hasta abril de 2026.

Lo que trae el registro y lo que no: no trae el NOMBRE del remitente, asi que
un saliente con usuario 0 por WhatsApp puede ser el bot o una persona. El
servidor lo guarda como "autor desconocido": saca al lead de "Sin responder"
pero no entra en la mediana. Ver _autor_de_registro en kommo.py.

Nunca pisa lo que trajo el webhook (mismo id de mensaje = se deja como esta).

Uso:
    $env:PULSE_TOKEN = "el ADMIN_TOKEN de Render"
    python scripts/rellenar_huecos.py                    # los huecos conocidos, solo cuenta
    python scripts/rellenar_huecos.py --cargar           # y los carga
    python scripts/rellenar_huecos.py tucoytico 2026-08-31T00:00 2026-09-02T18:00 [--cargar]

Las horas van en UTC. Los tokens de Kommo salen de .env.kommo; ninguno se
imprime.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
PULSE = 'https://kommo-webhook-mp4u.onrender.com'
TIPOS = '&filter[type][]=incoming_chat_message&filter[type][]=outgoing_chat_message'

# Huecos conocidos, en UTC, con margen a los dos lados: lo que ya esta guardado
# no se duplica, asi que sobrar no cuesta nada y faltar si.
TODAS = ['gruporegalado', 'ventasdirectas', 'tucoytico', 'corepowerconsulting',
         'autonica', 'lizondrohomes']
HUECOS = [(s, '2026-08-30T22:00', '2026-09-02T22:00') for s in TODAS] + [
    ('gruporegalado', '2026-08-27T22:00', '2026-08-29T08:00'),
    ('ventasdirectas', '2026-09-09T18:00', '2026-09-15T00:00'),
]


def tokens():
    out = {}
    with open(os.path.join(RAIZ, '.env.kommo'), encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith('#') and '=' in linea:
                sub, tok = linea.split('=', 1)
                out[sub.strip()] = tok.strip()
    return out


def pedir(url, token=None, cuerpo=None, timeout=120):
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
            # Sin la URL completa: la de PULSE lleva el token.
            raise SystemExit(f'HTTP {e.code} en {urllib.parse.urlsplit(url).path}')
    raise SystemExit('Kommo respondio 429 demasiadas veces')


def utc(txt):
    return int(datetime.strptime(txt, '%Y-%m-%dT%H:%M').replace(tzinfo=timezone.utc).timestamp())


def leer_registro(sub, token, desde, hasta):
    """Mensajes de chat de la ventana, ya traducidos a filas para PULSE."""
    entrantes, salientes, otros = [], [], Counter()
    pagina = 1
    while True:
        d = pedir(f'https://{sub}.kommo.com/api/v4/events?limit=100&page={pagina}{TIPOS}'
                  f'&filter[created_at][from]={desde}&filter[created_at][to]={hasta}', token)
        evs = (d.get('_embedded') or {}).get('events', [])
        for e in evs:
            if e.get('entity_type') != 'lead' or not e.get('entity_id'):
                otros[e.get('entity_type')] += 1
                continue
            if e['type'] == 'incoming_chat_message':
                entrantes.append({'lead_id': e['entity_id'], 'ts': e['created_at']})
            else:
                msg = ((e.get('value_after') or [{}])[0] or {}).get('message') or {}
                if not msg.get('id'):
                    otros['saliente sin id'] += 1
                    continue
                salientes.append({'msg_id': msg['id'], 'lead_id': e['entity_id'],
                                  'ts': e['created_at'], 'user_id': e.get('created_by') or 0,
                                  'origin': msg.get('origin'), 'talk_id': msg.get('talk_id')})
        if not evs or not (d.get('_links') or {}).get('next'):
            break
        pagina += 1
        time.sleep(0.12)
    return entrantes, salientes, otros


def un_hueco(sub, token, desde_txt, hasta_txt, pulse_token, cargar):
    ent, sal, otros = leer_registro(sub, token, utc(desde_txt), utc(hasta_txt))
    usuario0 = sum(1 for s in sal if not s['user_id'])
    print(f'{sub:20} {desde_txt} -> {hasta_txt} UTC | del cliente {len(ent):>6} | '
          f'salientes {len(sal):>6} (usuario 0: {usuario0})'
          + (f' | descartados {dict(otros)}' if otros else ''), flush=True)
    if not cargar:
        return
    tot = Counter()
    q = urllib.parse.urlencode({'token': pulse_token})
    lotes = max((len(ent) + 4999) // 5000, (len(sal) + 4999) // 5000, 1)
    for i in range(lotes):
        r = pedir(f'{PULSE}/cargar-mensajes?{q}', cuerpo={
            'subdomain': sub,
            'entrantes': ent[i * 5000:(i + 1) * 5000],
            'salientes': sal[i * 5000:(i + 1) * 5000]})
        if r.get('error'):
            raise SystemExit(f'PULSE rechazo la carga: {r["error"]}')
        tot.update({k: v for k, v in r.items() if isinstance(v, int)})
    print(f'{"":20} cargados: {tot["entrantes_nuevos"]} del cliente nuevos, '
          f'{tot["salientes_nuevos"]} salientes nuevos '
          f'({tot["salientes_autor_desconocido"]} con autor desconocido), '
          f'{tot["invalidos"]} invalidos', flush=True)


def main():
    cargar = '--cargar' in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    pulse_token = os.environ.get('PULSE_TOKEN', '').strip()
    if cargar and not pulse_token:
        raise SystemExit('Falta PULSE_TOKEN (el ADMIN_TOKEN de Render).')
    huecos = [(args[0], args[1], args[2])] if len(args) == 3 else HUECOS
    toks = tokens()
    for sub, desde, hasta in huecos:
        if sub not in toks:
            print(f'{sub}: sin token en .env.kommo')
            continue
        un_hueco(sub, toks[sub], desde, hasta, pulse_token, cargar)
    if not cargar:
        print('\n(solo lectura: no se cargo nada. Repetir con --cargar)')


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""Suscribe el webhook de PULSE a los eventos de conversacion: add_talk y update_talk.

Por que existe: "Sin responder" contaba leads cuya conversacion ya estaba
CERRADA en Kommo —el asesor la cerro porque no hacia falta contestar—. Medido
el 15/09/2026 en Camara China: 265 de 556 leads (48%), y 263 de 497 criticos.
Kommo avisa el cierre con update_talk: is_in_work pasa de 1 a 0.

Mismas precauciones que recortar_eventos.py, porque POST /api/v4/webhooks
REEMPLAZA la lista de eventos de esa URL, no la modifica:

  - primero se LEE lo que hay y se SUMAN los dos eventos, sin quitar nada,
  - no se manda nada sin --activar,
  - y despues se vuelve a leer de la API para verificar.

OJO: si el webhook esta APAGADO, guardarlo de nuevo puede reactivarlo (es lo
mismo que apretar Guardar en la pantalla de Kommo). Se avisa antes y se
muestra el estado despues.

Uso:
    python scripts/suscribir_conversaciones.py todas                    # solo mira
    python scripts/suscribir_conversaciones.py gruporegalado --activar
    python scripts/suscribir_conversaciones.py todas --activar

Los tokens salen de .env.kommo (una linea por cuenta). Nunca se imprimen.
"""
import os
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
RAIZ = os.path.dirname(AQUI)
sys.path.insert(0, AQUI)
from suscribir_salientes import DESTINO, pedir, webhooks_de

NUEVOS = ['add_talk', 'update_talk']


def tokens():
    out = {}
    with open(os.path.join(RAIZ, '.env.kommo'), encoding='utf-8') as f:
        for linea in f:
            linea = linea.strip()
            if linea and not linea.startswith('#') and '=' in linea:
                sub, tok = linea.split('=', 1)
                out[sub.strip()] = tok.strip()
    return out


def nuestro(subdomain, token):
    return next((w for w in webhooks_de(pedir(subdomain, token))
                 if (w.get('destination') or '').rstrip('/') == DESTINO.rstrip('/')), None)


def estado(w):
    return 'APAGADO' if (w or {}).get('disabled') else 'activo'


def una_cuenta(subdomain, token, activar):
    print(f'\n{subdomain}\n' + '=' * 66)
    w = nuestro(subdomain, token)
    if w is None:
        print(f'  No hay webhook apuntando a {DESTINO}. No se toca nada.')
        return False

    eventos = sorted(w.get('settings') or [])
    faltan = [e for e in NUEVOS if e not in eventos]
    print(f'  estado: {estado(w)}   eventos hoy: {len(eventos)}')
    if not faltan:
        print('  Ya tiene add_talk y update_talk. Nada que hacer.')
        return True

    finales = sorted(set(eventos) | set(NUEVOS))
    print(f'  faltan: {faltan}   quedaria con {len(finales)} eventos')
    if w.get('disabled'):
        print('  OJO: esta APAGADO. Guardarlo puede reactivarlo.')
    if not activar:
        print('  (solo lectura, no se cambio nada)')
        return True

    pedir(subdomain, token, 'POST', {'destination': DESTINO, 'settings': finales})

    # Verificar contra la API, no confiar en que el POST no dio error.
    despues = nuestro(subdomain, token) or {}
    ahora = set(despues.get('settings') or [])
    perdidos = sorted(set(eventos) - ahora)
    if perdidos:
        print(f'  CUIDADO: se perdieron eventos que antes estaban: {perdidos}')
        print('  Hay que volver a agregarlos ya mismo.')
        return False
    if not set(NUEVOS) <= ahora:
        print('  El POST no dio error pero los eventos no quedaron. Revisar a mano.')
        return False
    print(f'  Listo: {len(ahora)} eventos, con add_talk y update_talk. Estado ahora: {estado(despues)}')
    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cual = sys.argv[1]
    activar = '--activar' in sys.argv
    toks = tokens()
    cuentas = list(toks) if cual == 'todas' else [cual]
    ok = True
    for sub in cuentas:
        token = toks.get(sub) or os.environ.get('KOMMO_TOKEN', '').strip()
        if not token:
            print(f'\n{sub}: sin token en .env.kommo')
            ok = False
            continue
        ok = una_cuenta(sub, token, activar) and ok
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()

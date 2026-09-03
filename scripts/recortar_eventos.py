# -*- coding: utf-8 -*-
"""Deja el webhook suscrito SOLO a los eventos que el dashboard usa.

Por que existe: estabamos suscritos a 60 eventos de Kommo y leemos 5. Los
otros 55 —talk, contacts, notes, tasks, catalogos— llegaban igual y se
guardaban enteros en 'eventos' con tipo_evento='desconocido': 278.147 filas
que nadie releyo nunca. El 31/08/2026 la base llego al limite, Supabase la
puso en modo solo lectura y estuvimos 43 horas recibiendo webhooks sin
guardar uno solo.

Recortar la suscripcion ataca las dos causas a la vez: deja de crecer el
disco y baja el volumen de POST, que es la otra sospecha de por que Kommo
viene desactivando webhooks en las rafagas.

CUIDADO, esto es lo importante de este script: POST /api/v4/webhooks REEMPLAZA
la lista de eventos de esa URL, no la modifica. Un POST mal armado deja el
webhook suscrito a nada y se cae el dashboard entero. Por eso:

  - primero se LEE lo que hay,
  - se calcula la lista final y se muestra,
  - no se manda nada sin --aplicar,
  - y despues de aplicar se vuelve a leer de la API para verificar, en vez de
    confiar en que el POST no dio error.

Uso:
    $env:KOMMO_TOKEN = "..."                      # opcional, ver abajo
    python scripts/recortar_eventos.py tucoytico            # solo mira
    python scripts/recortar_eventos.py tucoytico --aplicar  # recien aca cambia
    python scripts/recortar_eventos.py todas                # las seis, solo mira

El token sale de .env.kommo (una linea por cuenta), o de KOMMO_TOKEN si se
paso a mano. Nunca se imprime, ni truncado.
"""
import json
import os
import sys
import urllib.error
import urllib.request

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(RAIZ, 'scripts'))
from bajar_asesores import _tokens

DESTINO = 'https://kommo-webhook-mp4u.onrender.com/webhook'

# Los unicos que _procesar_webhook sabe leer. Si alguna vez se agrega un
# handler nuevo en kommo.py, hay que agregarlo aca tambien: esta lista es la
# que decide que llega.
#
#   add_message           -> mensajes del cliente (mensajes_cliente)
#   add_outgoing_message  -> respuestas del asesor (mensajes_asesor)
#   update_lead           -> estado del lead, campos de fecha, embudo, asesor
#   status_lead           -> cambios de etapa. Se conserva por seguridad: el
#                            status llega tambien en update_lead, pero no hay
#                            garantia de que Kommo mande los dos siempre.
#   responsible_lead      -> cambios de responsable, misma razon.
QUEDARSE = [
    'add_message',
    'add_outgoing_message',
    'update_lead',
    'status_lead',
    'responsible_lead',
]

# Sin estos tres el dashboard no tiene de donde sacar los numeros. Si el
# recorte los dejaria afuera, el script se planta antes de mandar nada.
IMPRESCINDIBLES = {'add_message', 'add_outgoing_message', 'update_lead'}


def pedir(subdomain, token, metodo='GET', cuerpo=None):
    url = f'https://{subdomain}.kommo.com/api/v4/webhooks'
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(url, data=datos, method=metodo, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            crudo = r.read().decode()
            return json.loads(crudo) if crudo.strip() else {}
    except urllib.error.HTTPError as e:
        print(f'\nERROR HTTP {e.code} en {metodo} {url}')
        if e.code == 401:
            print('  El token no sirve o no es de esta cuenta.')
        print(f'  Respuesta: {e.read().decode()[:400]}')
        sys.exit(1)
    except Exception as e:
        print(f'\nERROR de red: {e}')
        sys.exit(1)


def nuestro_webhook(respuesta):
    for w in (respuesta.get('_embedded') or {}).get('webhooks') or []:
        if (w.get('destination') or '').rstrip('/') == DESTINO.rstrip('/'):
            return w
    return None


def una_cuenta(subdomain, token, aplicar):
    print(f'\n{subdomain}')
    print('=' * 70)

    w = nuestro_webhook(pedir(subdomain, token))
    if w is None:
        print(f'  No hay webhook apuntando a {DESTINO}. No se toca nada.')
        return False

    if w.get('disabled'):
        print('  OJO: este webhook esta APAGADO en Kommo. Recortarlo no lo '
              'prende; hay que reactivarlo aparte.')

    ahora = sorted(w.get('settings') or [])
    finales = sorted(e for e in QUEDARSE if e in ahora)
    sobran = sorted(set(ahora) - set(finales))

    faltan = IMPRESCINDIBLES - set(finales)
    if faltan:
        print(f'  NO SE TOCA: faltarian eventos imprescindibles: {sorted(faltan)}')
        print('  Esta cuenta no esta suscrita a todo lo que el dashboard '
              'necesita. Hay que arreglar eso antes de recortar.')
        return False

    print(f'  hoy: {len(ahora)} eventos    quedarian: {len(finales)}    '
          f'se van: {len(sobran)}')
    print(f'  se QUEDAN : {finales}')
    print(f'  se VAN    : {sobran}')

    if not aplicar:
        print('  (solo lectura, no se cambio nada)')
        return False

    print('  aplicando...')
    pedir(subdomain, token, 'POST', {'destination': DESTINO, 'settings': finales})

    # Verificar contra la API. Un POST sin error no prueba que quedo bien.
    despues = nuestro_webhook(pedir(subdomain, token))
    quedo = sorted((despues or {}).get('settings') or [])
    if set(quedo) != set(finales):
        print(f'  MAL: quedo {quedo}, se esperaba {finales}.')
        print('  Revisar a mano en Kommo AHORA.')
        sys.exit(1)
    print(f'  ok: {len(quedo)} eventos, verificado contra la API.')
    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cual = sys.argv[1]
    aplicar = '--aplicar' in sys.argv

    tokens = _tokens()
    if cual == 'todas':
        if aplicar:
            print('--aplicar no se admite con "todas". Se hace cuenta por')
            print('cuenta, verificando cada una, porque un POST mal armado')
            print('deja el webhook suscrito a nada.')
            sys.exit(1)
        objetivo = tokens
    else:
        t = os.environ.get('KOMMO_TOKEN', '').strip() or tokens.get(cual)
        if not t:
            print(f'No hay token para "{cual}". Ponelo en .env.kommo o en '
                  f'KOMMO_TOKEN.')
            sys.exit(1)
        objetivo = {cual: t}

    cambiadas = 0
    for sub, tok in objetivo.items():
        if una_cuenta(sub, tok, aplicar):
            cambiadas += 1

    print()
    if aplicar:
        print(f'Listo. Cuentas recortadas: {cambiadas}.')
        print('Verificar en /health/carga que "guardando" siga en true y que')
        print('los webhooks sigan llegando.')
    else:
        print('Nada de esto se aplico. Repetir con --aplicar, de a una cuenta.')


if __name__ == '__main__':
    main()

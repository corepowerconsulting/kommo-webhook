"""Suscribe el webhook de Kommo a los mensajes SALIENTES del asesor.

Por que un script y no la pantalla de Kommo: el evento add_outgoing_message no
aparece en la interfaz, solo se puede activar por la API.

Por que con tanto cuidado: POST /api/v4/webhooks REEMPLAZA la lista de eventos
de esa URL, no agrega. Mandar solo add_outgoing_message dejaria el webhook
suscrito UNICAMENTE a eso, y perderiamos lead_update, message y todo lo demas
—o sea, el dashboard entero—. Por eso primero se lee lo que hay y despues se
manda lo que habia MAS el nuevo.

El token nunca se escribe en el comando: se lee de una variable de entorno,
asi no queda en el historial de la consola.

    # 1. Ver que hay hoy (no cambia nada)
    $env:KOMMO_TOKEN = "el-token-largo"
    python scripts/suscribir_salientes.py corepowerconsulting

    # 2. Recien cuando el paso 1 se ve bien
    python scripts/suscribir_salientes.py corepowerconsulting --activar
"""

import json
import os
import sys
import urllib.error
import urllib.request

NUEVO = 'add_outgoing_message'
DESTINO = 'https://kommo-webhook-mp4u.onrender.com/webhook'


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
        detalle = e.read().decode()[:400]
        print(f'\nERROR HTTP {e.code} en {metodo} {url}')
        if e.code == 401:
            print('  El token no sirve o no es de esta cuenta. Cada subdominio')
            print('  de Kommo necesita el suyo.')
        print(f'  Respuesta: {detalle}')
        sys.exit(1)
    except Exception as e:
        print(f'\nERROR de red: {e}')
        sys.exit(1)


def webhooks_de(respuesta):
    """La API devuelve {_embedded: {webhooks: [...]}}, o vacio si no hay."""
    if not respuesta:
        return []
    return (respuesta.get('_embedded') or {}).get('webhooks') or []


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    subdomain = sys.argv[1]
    activar = '--activar' in sys.argv

    token = os.environ.get('KOMMO_TOKEN', '').strip()
    if not token:
        print('Falta el token. Antes de correr esto:')
        print('    $env:KOMMO_TOKEN = "el-token-largo-de-kommo"')
        print('\nSe saca en Kommo: Ajustes -> Integraciones -> crear una')
        print('integracion propia -> token de larga duracion.')
        sys.exit(1)

    print(f'Cuenta: {subdomain}')
    print('=' * 66)

    actuales = webhooks_de(pedir(subdomain, token))
    if not actuales:
        print('No hay NINGUN webhook configurado en esta cuenta.')
        print('Raro, porque estamos recibiendo eventos. Verifica el subdominio.')
        sys.exit(1)

    nuestro = None
    for w in actuales:
        print(f"\n  {w.get('destination')}")
        for ev in sorted(w.get('settings') or []):
            print(f'      - {ev}')
        if (w.get('destination') or '').rstrip('/') == DESTINO.rstrip('/'):
            nuestro = w

    if nuestro is None:
        print(f'\nNo se encontro un webhook apuntando a:\n  {DESTINO}')
        print('Sin eso no se puede saber que eventos conservar. No se toca nada.')
        sys.exit(1)

    eventos = list(nuestro.get('settings') or [])
    print('\n' + '=' * 66)
    print(f'El nuestro tiene {len(eventos)} eventos.')

    if NUEVO in eventos:
        print(f'{NUEVO} YA ESTA activo. No hay nada que hacer.')
        return

    finales = sorted(set(eventos) | {NUEVO})
    print(f'Quedaria con {len(finales)}: los {len(eventos)} de ahora mas {NUEVO}.')

    if not activar:
        print('\nEsto fue solo lectura, no se cambio nada.')
        print('Si la lista de arriba es la correcta, corre de nuevo con --activar')
        return

    print('\nAplicando...')
    pedir(subdomain, token, 'POST', {'destination': DESTINO, 'settings': finales})

    # Verificar contra la API, no confiar en que el POST no dio error.
    despues = webhooks_de(pedir(subdomain, token))
    quedo = next((w for w in despues
                  if (w.get('destination') or '').rstrip('/') == DESTINO.rstrip('/')), None)
    ahora = sorted((quedo or {}).get('settings') or [])

    perdidos = sorted(set(eventos) - set(ahora))
    if perdidos:
        print(f'\nCUIDADO: se perdieron eventos que antes estaban: {perdidos}')
        print('Hay que volver a agregarlos ya mismo.')
        sys.exit(1)
    if NUEVO not in ahora:
        print(f'\nEl POST no dio error pero {NUEVO} no quedo. Revisar a mano.')
        sys.exit(1)

    print(f'\nListo: {len(ahora)} eventos, con {NUEVO} incluido y sin perder ninguno.')
    print('Ahora responde un chat desde Kommo para generar uno de prueba.')


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""Baja los usuarios de una cuenta de Kommo y los imprime listos para pegar.

Hermano de bajar_embudos.py. El webhook manda user_id, no nombres, asi que la
lista tiene que estar en pulse_config.ASESORES para que el ranking muestre
gente y no "Usuario 6453065".

Ademas dice QUIEN de esa lista aparece de verdad respondiendo, cruzando contra
lo que capturamos: un usuario dado de alta que nunca contesto no es un asesor
del tablero, es una licencia.

Uso:
    python scripts/bajar_asesores.py tucoytico

El token sale de .env.kommo, en la raiz del repo, con una linea por cuenta:
    tucoytico=eyJ0eXAi...
Ese archivo esta en .gitignore (.env.*), asi que no se sube.
"""
import io
import json
import os
import sys
import urllib.request

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def token_de(sub):
    """Lee el token de esa cuenta desde .env.kommo. Nunca lo imprime."""
    ruta = os.path.join(RAIZ, '.env.kommo')
    if not os.path.exists(ruta):
        sys.exit(f'No existe {ruta}. Tiene que tener una linea "{sub}=<token>".')
    for linea in io.open(ruta, encoding='utf-8'):
        linea = linea.strip()
        if not linea or linea.startswith('#') or '=' not in linea:
            continue
        nombre, valor = linea.split('=', 1)
        if nombre.strip() == sub:
            return valor.strip().strip('"').strip("'")
    sys.exit(f'No hay linea "{sub}=..." en .env.kommo')


def pedir(sub, token, ruta):
    req = urllib.request.Request(
        f'https://{sub}.kommo.com{ruta}',
        headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode('utf-8'))


def main():
    if len(sys.argv) < 2:
        sys.exit('Uso: python scripts/bajar_asesores.py <subdomain>')
    sub = sys.argv[1].strip()
    token = token_de(sub)

    usuarios = []
    pagina = 1
    while True:
        try:
            d = pedir(sub, token, f'/api/v4/users?page={pagina}&limit=250')
        except Exception as e:
            sys.exit(f'Error consultando Kommo: {e}')
        lote = (d.get('_embedded') or {}).get('users') or []
        usuarios.extend(lote)
        if len(lote) < 250:
            break
        pagina += 1

    print('=' * 74)
    print(f'USUARIOS DE {sub} SEGUN KOMMO  ({len(usuarios)})')
    print('=' * 74)
    print(f'{"user_id":>10}  {"nombre":<34} {"email":<28}')
    print('-' * 74)
    for u in sorted(usuarios, key=lambda x: (x.get('name') or '').lower()):
        print(f'{u.get("id"):>10}  {(u.get("name") or "")[:34]:<34} {(u.get("email") or "")[:28]:<28}')

    # Cuales ya estan en nuestra lista y cuales faltan. Un id que falta sale en
    # el ranking como "Usuario 12345".
    sys.path.insert(0, RAIZ)
    from pulse_config import ASESORES
    ids = {u.get('id') for u in usuarios}
    faltan = [u for u in usuarios if u.get('id') not in ASESORES]
    print()
    print(f'ya estan en pulse_config.ASESORES: {len(ids & set(ASESORES))} de {len(ids)}')
    if faltan:
        print()
        print('FALTAN. Pegar en pulse_config.ASESORES:')
        for u in sorted(faltan, key=lambda x: (x.get('name') or '').lower()):
            nombre = (u.get('name') or '').replace("'", "\\'")
            print(f"    {u.get('id')}: '{nombre}',")
    else:
        print('No falta ninguno.')


if __name__ == '__main__':
    main()

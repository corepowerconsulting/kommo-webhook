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
    python scripts/bajar_asesores.py todas     <- las seis, consolidado

El token sale de .env.kommo, en la raiz del repo, con una linea por cuenta:
    tucoytico=eyJ0eXAi...
Ese archivo esta en .gitignore (.env.*), asi que no se sube.
"""
import html
import io
import json
import os
import sys
import urllib.request

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _tokens():
    """Todas las cuentas de .env.kommo. Devuelve {subdomain: token}.

    Nunca se imprime un token, ni truncado: la unica salida del script son
    nombres y numeros."""
    ruta = os.path.join(RAIZ, '.env.kommo')
    if not os.path.exists(ruta):
        sys.exit(f'No existe {ruta}. Una linea por cuenta: "<subdomain>=<token>".')
    fuera = {}
    for linea in io.open(ruta, encoding='utf-8'):
        linea = linea.strip()
        if not linea or linea.startswith('#') or '=' not in linea:
            continue
        nombre, valor = linea.split('=', 1)
        fuera[nombre.strip()] = valor.strip().strip('"').strip("'")
    return fuera


def token_de(sub):
    t = _tokens().get(sub)
    if not t:
        sys.exit(f'No hay linea "{sub}=..." en .env.kommo')
    return t


def pedir(sub, token, ruta):
    req = urllib.request.Request(
        f'https://{sub}.kommo.com{ruta}',
        headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode('utf-8'))


def usuarios_de(sub, token):
    usuarios, pagina = [], 1
    while True:
        d = pedir(sub, token, f'/api/v4/users?page={pagina}&limit=250')
        lote = (d.get('_embedded') or {}).get('users') or []
        usuarios.extend(lote)
        if len(lote) < 250:
            break
        pagina += 1
    return usuarios


def limpiar(nombre):
    """Kommo escapa los nombres para HTML: manda "Tuco&amp;Tico". Sin
    desescapar, ese &amp; termina impreso tal cual en el ranking."""
    return html.unescape((nombre or '').strip())


def main():
    if len(sys.argv) < 2:
        sys.exit('Uso: python scripts/bajar_asesores.py <subdomain>|todas')

    pedido = sys.argv[1].strip()
    tokens = _tokens()
    cuentas = list(tokens) if pedido == 'todas' else [pedido]
    if pedido != 'todas':
        token_de(pedido)          # valida que exista antes de empezar

    sys.path.insert(0, RAIZ)
    from pulse_config import ASESORES

    todo, faltantes = {}, []
    for sub in cuentas:
        try:
            todo[sub] = usuarios_de(sub, tokens[sub])
        except Exception as e:
            print(f'!! {sub}: no se pudo consultar ({e})')
            todo[sub] = None

    for sub in cuentas:
        us = todo[sub]
        if us is None:
            continue
        print()
        print('=' * 78)
        print(f'{sub}  —  {len(us)} usuarios en Kommo')
        print('=' * 78)
        print(f'{"user_id":>10}  {"nombre":<32} {"email":<30} en config')
        print('-' * 78)
        for u in sorted(us, key=lambda x: limpiar(x.get('name')).lower()):
            uid, nombre = u.get('id'), limpiar(u.get('name'))
            esta = 'si' if uid in ASESORES else 'FALTA'
            if uid not in ASESORES:
                faltantes.append((uid, nombre))
            print(f'{uid:>10}  {nombre[:32]:<32} {(u.get("email") or "")[:30]:<30} {esta}')

    # Un id que falta sale en el ranking como "Usuario 12345" en vez del nombre.
    print()
    print('=' * 78)
    total = sum(len(u) for u in todo.values() if u)
    print(f'TOTAL: {total} usuarios en {len([u for u in todo.values() if u])} cuentas · '
          f'{len(faltantes)} sin configurar')
    print('=' * 78)
    if faltantes:
        print('Pegar en pulse_config.ASESORES:')
        for uid, nombre in sorted(set(faltantes), key=lambda x: x[1].lower()):
            print(f"    {uid}: '{nombre}',")
    else:
        print('No falta ninguno.')


if __name__ == '__main__':
    main()

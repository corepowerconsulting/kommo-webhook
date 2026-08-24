"""Baja los NOMBRES de embudos, etapas y usuarios de una cuenta de Kommo.

Por que hace falta: el webhook manda numeros, no nombres. Un lead llega con
pipeline_id 7008155 y status_id 74938892, y sin esta tabla el dashboard solo
puede mostrar el numero. Los nombres estan en la API y no en el webhook.

Por que se baja una vez y se pega en pulse_config en vez de consultarlo en
vivo: los embudos casi no cambian, y depender de la API en cada carga del
dashboard agrega una llamada de red al camino critico y un token de larga
duracion en produccion. Es el mismo patron que ya usa ASESORES.

De paso baja los usuarios, que resuelve otro problema: la lista ASESORES se
mantiene a mano y ya tiene tres asesores faltantes que si mandan mensajes
(Oliric Lizondro, Margaret E. Ricart, Yealkis Soto).

El token nunca se escribe en el comando: se lee de una variable de entorno,
asi no queda en el historial de la consola.

    $env:KOMMO_TOKEN = "el-token-de-esa-cuenta"
    python scripts/bajar_embudos.py gruporegalado             # solo muestra
    python scripts/bajar_embudos.py gruporegalado --guardar    # ademas escribe

Con --guardar mezcla lo bajado dentro de embudos.json, sin tocar las otras
cuentas. Se escribe un archivo en vez de pegar a mano: Camara China sola tiene
24 embudos con cerca de 300 etapas, y transcribir eso garantiza erratas que
despues aparecen como una etapa con el nombre equivocado en el dashboard.

Este script no toca Kommo ni la base: lee de la API y, si se le pide, escribe
un archivo local.
"""

import json
import os
import sys
import urllib.error
import urllib.request

ARCHIVO = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       'embudos.json')


def pedir(subdomain, token, ruta):
    url = f'https://{subdomain}.kommo.com/api/v4/{ruta}'
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            if r.status == 204:
                return {}
            crudo = r.read().decode()
            return json.loads(crudo) if crudo.strip() else {}
    except urllib.error.HTTPError as e:
        print(f'\nERROR HTTP {e.code} en /{ruta}')
        if e.code == 401:
            print('  El token no sirve o es de otra cuenta. Cada subdominio')
            print('  de Kommo necesita el suyo.')
        elif e.code == 403:
            print('  El token no tiene permiso para leer esto.')
        print(f'  {e.read().decode()[:300]}')
        sys.exit(1)
    except Exception as e:
        print(f'\nERROR de red: {e}')
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    subdomain = sys.argv[1]
    token = os.environ.get('KOMMO_TOKEN', '').strip()
    if not token:
        print('Falta el token. Antes de correr esto:')
        print('    $env:KOMMO_TOKEN = "el-token-largo-de-esa-cuenta"')
        return 1

    # --- Embudos y etapas ----------------------------------------------------
    datos = pedir(subdomain, token, 'leads/pipelines')
    pipelines = (datos.get('_embedded') or {}).get('pipelines') or []

    print()
    print('# ' + '=' * 68)
    print(f'# Embudos y etapas de {subdomain}')
    print(f'# {len(pipelines)} embudos. Pegar en PULSE_CONFIG["{subdomain}"].')
    print('# ' + '=' * 68)
    print("    'embudos': {")
    for p in pipelines:
        estados = (p.get('_embedded') or {}).get('statuses') or []
        principal = ' (principal)' if p.get('is_main') else ''
        print(f"        {p['id']}: {{   # {p.get('name')}{principal}"
              f"  · {len(estados)} etapas")
        print(f"            'nombre': {json.dumps(p.get('name'), ensure_ascii=False)},")
        print("            'etapas': {")
        for e in sorted(estados, key=lambda x: x.get('sort', 0)):
            # 142 y 143 son constantes de Kommo iguales en toda cuenta:
            # ganado y perdido. El resto las arma cada cliente.
            fijo = '   # constante de Kommo' if e['id'] in (142, 143) else ''
            print(f"                {e['id']}: "
                  f"{json.dumps(e.get('name'), ensure_ascii=False)},{fijo}")
        print('            },')
        print('        },')
    print('    },')

    # --- Usuarios ------------------------------------------------------------
    datos = pedir(subdomain, token, 'users')
    usuarios = (datos.get('_embedded') or {}).get('users') or []

    print()
    print('# ' + '=' * 68)
    print(f'# Usuarios de {subdomain} ({len(usuarios)})')
    print('# Para ASESORES. Los ids de Kommo son unicos entre cuentas, asi que')
    print('# van todos al mismo diccionario plano.')
    print('# ' + '=' * 68)
    for u in sorted(usuarios, key=lambda x: x.get('name') or ''):
        activo = '' if u.get('rights', {}).get('is_active', True) else '   # INACTIVO'
        print(f"    {u['id']}: {json.dumps(u.get('name'), ensure_ascii=False)},{activo}")

    # --- Guardar -------------------------------------------------------------
    if '--guardar' in sys.argv:
        # Se MEZCLA, no se reemplaza: cada cuenta se baja por separado porque
        # cada una necesita su token, y escribir el archivo entero borraria las
        # que se bajaron antes.
        try:
            with open(ARCHIVO, encoding='utf-8') as f:
                todo = json.load(f)
        except (FileNotFoundError, ValueError):
            todo = {}

        todo[subdomain] = {
            'embudos': {
                str(p['id']): {
                    'nombre': p.get('name'),
                    'principal': bool(p.get('is_main')),
                    'etapas': {
                        str(e['id']): e.get('name')
                        for e in (p.get('_embedded') or {}).get('statuses') or []
                    },
                }
                for p in pipelines
            },
            'usuarios': {str(u['id']): u.get('name') for u in usuarios},
        }

        with open(ARCHIVO, 'w', encoding='utf-8') as f:
            json.dump(todo, f, ensure_ascii=False, indent=2, sort_keys=True)
        print()
        print(f'# Guardado en {ARCHIVO}')
        print(f'# Cuentas en el archivo: {", ".join(sorted(todo))}')

    print()
    print(f'# Listo. {len(pipelines)} embudos y {len(usuarios)} usuarios de {subdomain}.')
    if '--guardar' not in sys.argv:
        print('# No se escribio nada. Agrega --guardar para que se guarde.')
    return 0


if __name__ == '__main__':
    sys.exit(main())

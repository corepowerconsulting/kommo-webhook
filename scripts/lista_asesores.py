# -*- coding: utf-8 -*-
"""La lista completa de quien atiende, por cuenta, juntando las DOS fuentes.

  Kommo   los usuarios dados de alta en la cuenta (API /api/v4/users)
  Webhook quien mando mensajes salientes de verdad, de lo que capturamos

Las dos hacen falta porque no coinciden, y ahi esta lo interesante:

  Un usuario de Kommo que nunca contesto no es un asesor: es una licencia.
  Sale en el filtro de Responsables y nunca en el ranking.

  Un remitente que NO es usuario de Kommo —whatsappWZ, WhatsApp Lite,
  WhatsApp Business— es atencion real de una persona contestando desde la app
  o desde una integracion, pero Kommo registra el CANAL y no quien apreto
  enviar. Aparece en el ranking sin puesto, como "sin identificar".

Uso:
    $env:PULSE_ADMIN_TOKEN = "..."          # PowerShell
    export PULSE_ADMIN_TOKEN="..."          # bash
    python scripts/lista_asesores.py

Los tokens de Kommo salen de .env.kommo. Ninguno de los dos se imprime.
"""
import html as _html
import json
import os
import sys
import urllib.request
from collections import defaultdict

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROD = 'https://kommo-webhook-mp4u.onrender.com'

sys.path.insert(0, RAIZ)
sys.path.insert(0, os.path.join(RAIZ, 'scripts'))
from bajar_asesores import _tokens, usuarios_de, limpiar   # noqa: E402


def remitentes_de(sub, admin):
    """Quien mando salientes, de /health/remitentes. Lee mensajes_asesor, que
    esta indexada: responde en segundos aunque la cuenta tenga 130.000
    eventos."""
    url = f'{PROD}/health/remitentes?subdomain={sub}&token={admin}'
    try:
        with urllib.request.urlopen(url, timeout=90) as r:
            return json.loads(r.read().decode('utf-8')).get('remitentes', [])
    except Exception as e:
        print(f'   !! no se pudo consultar el webhook: {e}')
        return None


def main():
    admin = os.environ.get('PULSE_ADMIN_TOKEN', '').strip()
    if not admin:
        sys.exit('Falta PULSE_ADMIN_TOKEN en el entorno. Ver el encabezado.')

    tokens = _tokens()
    total_personas = total_canales = total_bots = total_licencias = 0
    # Que valores toma author[type] en la practica, y como se relaciona con
    # que el mensaje traiga o no un user_id. Es la pregunta que hizo el
    # cliente y no estaba medida: capturabamos el campo sin tabularlo.
    tipos = defaultdict(lambda: {'ident': 0, 'canal': 0, 'msjs': 0, 'ejemplos': set()})
    origenes = defaultdict(lambda: {'msjs': 0, 'ejemplos': set()})

    for sub in tokens:
        print()
        print('=' * 88)
        print(f'  {sub.upper()}')
        print('=' * 88)

        try:
            usuarios = usuarios_de(sub, tokens[sub])
        except Exception as e:
            print(f'   !! no se pudo consultar Kommo: {e}')
            usuarios = []
        por_id = {u['id']: limpiar(u.get('name')) for u in usuarios}

        rem = remitentes_de(sub, admin)
        if rem is None:
            continue
        if not rem:
            print('   Sin mensajes salientes capturados en esta cuenta.')
            print(f'   ({len(usuarios)} usuarios en Kommo, ninguno con mensajes nuestros)')
            continue

        print()
        print('  QUIEN CONTESTA DE VERDAD  (capturado por el webhook)')
        print(f'  {"mensajes":>8} {"leads":>6}  {"nombre":<32} {"user_id":>9}  como lo trata el ranking')
        print('  ' + '-' * 84)
        contestaron = set()
        for r in rem:
            # Tabulacion de author[type] y origin, cruzada con si el mensaje
            # vino identificado. Es lo que decide si un valor sirve para
            # distinguir "salio del CRM" de "salio de afuera".
            t = tipos[r['author_type'] or '(vacio)']
            t['msjs'] += r['mensajes']
            t['ident' if r['user_id'] else 'canal'] += 1
            if len(t['ejemplos']) < 4:
                t['ejemplos'].add(r['nombre_en_el_mensaje'])
            o = origenes[r['origin'] or '(vacio)']
            o['msjs'] += r['mensajes']
            if len(o['ejemplos']) < 4:
                o['ejemplos'].add(r['nombre_en_el_mensaje'])

            trato = r['lo_trata_como']
            if 'automático' in trato:
                total_bots += 1
            elif 'sin identificar' in trato:
                total_canales += 1
            else:
                total_personas += 1
                if r['user_id']:
                    contestaron.add(r['user_id'])
            aviso = '' if r['en_pulse_config'] or not r['user_id'] else '  <-- FALTA en pulse_config'
            print(f'  {r["mensajes"]:>8} {r["leads"]:>6}  {r["nombre_en_el_tablero"][:32]:<32} '
                  f'{r["user_id"] or "":>9}  {trato}{aviso}')

        # Dados de alta en Kommo que nunca mandaron un mensaje que hayamos
        # capturado. Ocupan lugar en el filtro y nunca en el ranking.
        licencias = sorted(n for i, n in por_id.items() if i not in contestaron)
        if licencias:
            total_licencias += len(licencias)
            print()
            print(f'  EN KOMMO PERO SIN MENSAJES CAPTURADOS  ({len(licencias)} de {len(por_id)})')
            for i in range(0, len(licencias), 3):
                print('    ' + ' · '.join(licencias[i:i + 3]))

    print()
    print('=' * 88)
    print(f'  personas que contestan : {total_personas}')
    print(f'  canales sin identificar: {total_canales}   (whatsappWZ, WhatsApp Lite y parecidos)')
    print(f'  bots                   : {total_bots}')
    print(f'  usuarios sin actividad : {total_licencias}')
    print('=' * 88)

    # --- Que valores toma author[type], y si sirven para decidir ------------
    # La pregunta concreta: ¿alcanza author[type] para saber si el mensaje
    # salio del CRM o de afuera? Se responde viendo si cada valor aparece SOLO
    # con user_id o SOLO sin el. Si un mismo valor aparece de los dos lados, no
    # sirve como condicion.
    print()
    print('=' * 88)
    print('  author[type]: que valores llegan y si distinguen CRM de canal')
    print('=' * 88)
    print(f'  {"author_type":<16} {"mensajes":>9} {"identif.":>9} {"canal":>7}  {"¿sirve?":<12} ejemplos')
    print('  ' + '-' * 84)
    for t, d in sorted(tipos.items(), key=lambda x: -x[1]['msjs']):
        puro = 'si, solo CRM' if not d['canal'] else ('si, solo canal' if not d['ident'] else 'NO, mezcla')
        print(f'  {t:<16} {d["msjs"]:>9} {d["ident"]:>9} {d["canal"]:>7}  {puro:<12} '
              f'{", ".join(sorted(d["ejemplos"]))[:34]}')

    print()
    print('=' * 88)
    print('  origin: por donde salio')
    print('=' * 88)
    for o, d in sorted(origenes.items(), key=lambda x: -x[1]['msjs']):
        print(f'  {o:<26} {d["msjs"]:>8} mensajes   {", ".join(sorted(d["ejemplos"]))[:38]}')


if __name__ == '__main__':
    main()

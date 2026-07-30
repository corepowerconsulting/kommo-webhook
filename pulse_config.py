# Los user_id de Kommo son unicos entre cuentas, no por cuenta: de los 13 ids
# vistos en los 4 subdominios, el unico que se repite es 6767570, y se repite
# porque es realmente la misma persona (el soporte de la consultora, que tiene
# usuario en el CRM de cada cliente). Por eso un solo diccionario plano alcanza
# y no hace falta anidar por subdominio.
#
# Agrupados por cuenta solo para que se lea; la busqueda es por id.
# Si un asesor no esta aca, el dashboard muestra 'Usuario <id>' al cliente.
ASESORES = {
    # Core Power Consulting (soporte: tiene usuario en las 4 cuentas)
    6767570:  'Soporte Core Power Consulting',
    6453065:  'Luis Marroquín',
    11579748: 'Giulliana Vera',
    15541872: 'Ana Mirian Moran',

    # Autonica
    15080019: 'Auto Nica',
    15081799: 'Citas 1',
    15093487: 'Citas 2',
    15093495: 'Citas 3',

    # TuCoyTico
    13478031: 'Ventas Tuco & Tico',

    # Camara de Comercio China Ecuador (gruporegalado)
    10166363: 'Pablo Regalado',
    10914216: 'Ventas',
    14483404: 'Ventas 2',
    14483200: 'Ventas 3',
    9750963:  'Marketing',
    15309624: 'Marketing ADS',
    9750975:  'Administrador',

    # Ventas Directas
    13127364: 'Maria Ventura',
    13127368: 'Mayte Jimenez',
    13902356: 'Freddy Hernandez',
    9482679:  'Sebastian Millan',
    9494503:  'Paul Adams',
}

PULSE_CONFIG = {
    'corepowerconsulting': {
        'nombre': 'Core Power Consulting',
        'crm_domain': 'corepowerconsulting.kommo.com',
        'campos': {'cliente': 'F Ult msj cliente', 'asesor': 'F ult msj asesor'},
        'tz_offset': -5,
        'horario': (5, 18),
        'dias_laborables': [0, 1, 2, 3, 4],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#22c55e', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
    'autonica': {
        'nombre': 'Autonica',
        'crm_domain': 'autonica.kommo.com',
        'campos': {'cliente': 'F ult msj cliente seg', 'asesor': 'F ult msj asesor'},
        'tz_offset': -5,
        'horario': (8, 18),
        'dias_laborables': [0, 1, 2, 3, 4, 5],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#22c55e', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
    'tucoytico': {
        'nombre': 'TuCoyTico',
        'crm_domain': 'tucoytico.kommo.com',
        'campos': {'cliente': 'F ult msj cliente', 'asesor': 'F ult msj asesor'},
        'tz_offset': -6,
        'horario': (8, 18),
        'dias_laborables': [0, 1, 2, 3, 4, 5],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#22c55e', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
    'ventasdirectas': {
        'nombre': 'Ventas Directas',
        'crm_domain': 'ventasdirectas.kommo.com',
        'campos': {'cliente': 'F utl msj cliente', 'asesor': 'F utl msj Asesor'},
        'tz_offset': -5,
        'horario': (8, 18),
        'dias_laborables': [0, 1, 2, 3, 4],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#22c55e', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },

    # ------------------------------------------------------------------
    # Cuentas nuevas (alta 29/07/2026). Ecuador = UTC-5, sin horario de verano.
    #
    # PENDIENTE en las dos: los nombres de 'campos' son los que se usan por
    # defecto, todavia NO verificados contra los payloads reales. Cada cuenta de
    # Kommo nombra distinto esos campos y un nombre equivocado no da error: el
    # dashboard queda en cero y nada lo indica. Confirmar con
    # /health/alta?subdomain=<cuenta> en cuanto lleguen eventos, y con
    # /health/campos, que reporta ok:false si no los encuentra.
    #
    # El horario 8-18 Lun-Vie es el mas comun entre las otras cuentas, tomado
    # como punto de partida. Hay que confirmarlo con el cliente porque define
    # que horas se descuentan del tiempo de respuesta.
    # ------------------------------------------------------------------
    'gruporegalado': {
        'nombre': 'Cámara de Comercio China Ecuador',
        'crm_domain': 'gruporegalado.kommo.com',
        # 'cliente' CONFIRMADO: aparece con valor en 19 de 19 eventos revisados.
        # 'asesor' se llama distinto que en las otras cuentas. No lo vimos aun en
        # ningun payload (la cuenta lleva minutos conectada), pero el log de
        # Kommo lo muestra explicito:
        #   SalesBot (BOTFULTMSJASESOR)
        #   The value of the field «F ult msj enviado» is set to «28.07.2026 18:38»
        # La comparacion es exacta y sensible a mayusculas, asi que si el nombre
        # real difiere en una letra esto no mide nada y no avisa. Confirmar con
        # /health/campos, que reporta ok:false mientras no lo encuentre.
        'campos': {'cliente': 'F Ult msj cliente', 'asesor': 'F ult msj enviado'},
        'tz_offset': -5,
        'horario': (8, 18),
        'dias_laborables': [0, 1, 2, 3, 4],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#22c55e', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
    'lizondrohomes': {
        'nombre': 'Lizandro Homes',
        'crm_domain': 'lizondrohomes.kommo.com',
        'campos': {'cliente': 'F Ult msj cliente', 'asesor': 'F ult msj asesor'},
        'tz_offset': -5,
        'horario': (8, 18),
        'dias_laborables': [0, 1, 2, 3, 4],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#22c55e', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
}

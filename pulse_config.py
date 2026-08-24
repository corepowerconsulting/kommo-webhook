# Los user_id de Kommo son unicos entre cuentas, no por cuenta: de los 13 ids
# vistos en los 4 subdominios, el unico que se repite es 6767570, y se repite
# porque es realmente la misma persona (el soporte de la consultora, que tiene
# usuario en el CRM de cada cliente). Por eso un solo diccionario plano alcanza
# y no hace falta anidar por subdominio.
#
# Agrupados por cuenta solo para que se lea; la busqueda es por id.
# Si un asesor no esta aca, el dashboard muestra 'Usuario <id>' al cliente.
# Bajados de la API con scripts/bajar_embudos.py el 2026-08-24, no escritos a
# mano: la lista tenia 21 nombres y le faltaban tres que SI mandan mensajes
# (Oliric Lizondro, Margaret E. Ricart, Yealkis Soto), que salian en el
# dashboard como "Usuario 15684248".
#
# Se conservan los usuarios INACTIVOS y los que ya no estan en la API. Los
# datos historicos siguen refiriendolos, y sin el nombre esas filas quedarian
# con un numero. En Camara China eso importa especialmente: casi todos los
# asesores con nombre propio estan inactivos y trabajan bajo el usuario
# compartido "Ventas".
ASESORES = {
    # Core Power Consulting (soporte: tiene usuario en las 6 cuentas)
    6767570:  'Soporte Core Power Consulting',
    6453065:  'Luis Marroquín',
    11579748: 'Giulliana Vera',
    15541872: 'Ana Miriam Morán',
    # Segundo usuario de la misma persona, de antes. Los datos viejos lo
    # referencian, asi que se conservan los dos ids.
    6550475:  'Ana Miriam Morán',
    11606140: 'Jonathan Urbina',
    9601187:  'Maria Fernanda Morillo',
    6731147:  'Luis',

    # Autonica
    15080019: 'Auto Nica',
    15081799: 'Citas 1',
    15093487: 'Citas 2',
    15093495: 'Citas 3',

    # TuCoyTico
    13478031: 'Ventas Tuco & Tico',
    # La API lo devuelve escapado para HTML ("Tuco&amp;Tico"); aca va limpio.
    13467643: 'Tuco & Tico',

    # Lizondro Homes
    11447079: 'Oliric Lizondro',

    # Camara de Comercio China Ecuador (gruporegalado)
    10914216: 'Ventas',
    14483404: 'Ventas 2',
    14483200: 'Ventas 3',
    9750963:  'Marketing',
    15309624: 'Marketing ADS',
    15308260: 'Marketing Campañas',
    9750975:  'Administrador',
    10166363: 'Pablo Regalado',
    9442311:  'Jorge Regalado',
    9750987:  'Darío Regalado',
    9750991:  'Daniel Benítez',
    9914268:  'Karina Vivas',
    9780683:  'Sami Cachiguango',
    9780687:  'Liliana Pachacama',
    10090311: 'Lizeth Belén Villarroel Naranjo',
    9774003:  'Margarita Carpio',
    9773999:  'Marly Moran',
    9773995:  'Nicolas Vacacela',
    9780707:  'Carlos Castro',
    10722348: 'Cecilia Noboa',
    9407311:  'Información Grupo Regalado',
    9655271:  'Camara de Comercio Ecuador Shanghai',
    9751875:  '◈ Alibox ◈',

    # Ventas Directas
    15684248: 'Margaret E. Ricart',
    15694620: 'Yealkis Soto',
    9482679:  'Sebastian Millan',
    9494503:  'Paul Adams',
    9562043:  'Johan Manrique',
    10152363: 'Peterson',
    # Ya no estan en la API —los borraron de Kommo— pero los datos viejos
    # todavia los referencian.
    13127364: 'Maria Ventura',
    13127368: 'Mayte Jimenez',
    13902356: 'Freddy Hernandez',
}

# Remitentes AUTOMATICOS: mensajes salientes que no los escribio una persona.
#
# Con los mensajes salientes capturados, cada respuesta trae el nombre de quien
# la mando. Estos tres son bots, y se sabe por el tiempo que tardan: en
# tucoytico el Salesbot responde con una MEDIANA de 4 segundos, Instagram 4 y
# Facebook 5. Ninguna persona contesta asi.
#
# No se usa author][user_id] = 0 para decidirlo, aunque seria mas comodo: el
# asesor que contesta desde la app de WhatsApp tambien llega con user_id 0 y
# nombre 'WhatsApp Business', y su mediana es de 15 minutos con un p90 de 37
# horas. Filtrar por user_id descartaria 538 respuestas humanas en tucoytico.
#
# Para que sirve: estos aparecen en el ranking como una fila propia —para poder
# ver cuanto responde el bot y a cuantos— pero NO entran en el numero grande ni
# en la distribucion, porque si entraran la mediana de tucoytico caeria de
# 1h 01m a 3m 38s y diria que atendemos en tres minutos, que es falso: el
# cliente sigue esperando a una persona.
#
# Si aparece un bot nuevo se agrega aca. La pista es siempre la misma: una
# mediana de segundos.
REMITENTES_AUTOMATICOS = {'Salesbot', 'Instagram', 'Facebook'}

# tz_offset: la diferencia con UTC de la ciudad donde atiende cada cuenta. Se
# usa para dos cosas: mostrar las horas y decidir si un mensaje entro dentro
# del horario laboral. Si esta mal, las horas del dashboard no coinciden con
# las que muestra Kommo y ademas se computan mal las esperas, porque un
# mensaje de las 17:50 se toma como de las 18:50 y el reloj no arranca hasta
# el dia siguiente.
#
# Ninguno de estos paises usa horario de verano, asi que el numero es fijo:
#   El Salvador -6   ·   Panama -5   ·   Ecuador -5   ·   Rep. Dominicana -4
PULSE_CONFIG = {
    'corepowerconsulting': {
        'nombre': 'Core Power Consulting',
        'crm_domain': 'corepowerconsulting.kommo.com',
        'campos': {'cliente': 'F Ult msj cliente', 'asesor': 'F ult msj asesor'},
        # San Salvador. Estaba en -5 y el dashboard mostraba todo una hora
        # adelantado respecto de Kommo; lo detecto Juan comparando un mensaje.
        'tz_offset': -6,
        # Confirmado con Juan: atienden de verdad de 5 de la mañana a 6 de la
        # tarde. No es una compensacion de la zona horaria que estaba mal.
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
        # San Salvador, igual que Core Power. Estaba en -5.
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
    'tucoytico': {
        'nombre': 'TuCoyTico',
        'crm_domain': 'tucoytico.kommo.com',
        'campos': {'cliente': 'F ult msj cliente', 'asesor': 'F ult msj asesor'},
        # San Salvador. Era la unica de las tres de El Salvador que ya estaba
        # bien; las otras dos estaban en -5.
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
        # Santo Domingo. Estaba en -5, o sea una hora ATRASADO — al reves que
        # las dos de El Salvador, que estaban adelantadas.
        'tz_offset': -4,
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
        # Quien atendio DE VERDAD. Esta cuenta comparte el usuario "Ventas"
        # entre Marilyn, Denise y otros, asi que el responsible_user_id de
        # Kommo no distingue a nadie: todo el trabajo de tres personas aparece
        # bajo un solo nombre.
        #
        # Este campo custom si trae la persona. Medido sobre 50 lead_update:
        # 26 lo traen con valor, con 7 nombres distintos (Ivis Anchundia,
        # Gisela Alvarez, Karina Vivas, Damaris Ñacato, Jhonny Lopez, Daniel
        # Benitez, Sami Cachiguango). Los que no lo traen caen al responsable,
        # como antes.
        #
        # Es un campo de TEXTO: el valor esta en [values][0][value], no en
        # [values][0] como los de fecha.
        #
        # Se llama 'campo_quien_atendio' y no 'campo_asesor' a proposito: justo
        # arriba, campos['asesor'] es la FECHA del ultimo mensaje del asesor.
        # Dos claves con la palabra "asesor" que significan cosas distintas se
        # confunden sola.
        'campo_quien_atendio': 'Asesor',
        # Ecuador. Verificado, no hace falta cambiarlo.
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
        # Los dos CONFIRMADOS contra payloads reales. 'cliente' va con u
        # minuscula: se habia configurado 'F Ult msj cliente' y por esa unica
        # letra la cuenta media cero, sin ningun error. La comparacion de
        # get_custom_field es exacta.
        'campos': {'cliente': 'F ult msj cliente', 'asesor': 'F ult msj asesor'},
        # Panama, que tambien es -5. Verificado, no hace falta cambiarlo.
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

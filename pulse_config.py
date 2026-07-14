ASESORES = {
    6767570: 'Soporte Core Power Consulting',
    6453065: 'Luis Marroquín',
    11579748: 'Giulliana Vera',
    15541872: 'Ana Mirian Moran',
    15080019: 'Auto Nica',
    13127364: 'Maria Ventura',
    9482679: 'Sebastian Millan',
    9494503: 'Paul Adams',
    13127368: 'Mayte Jimenez',
    13902356: 'Freddy Hernandez',
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
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#10b981', 'tag': 'Ideal'},
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
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#10b981', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
    'tucoytico': {
        'nombre': 'Tu Coy Tico',
        'crm_domain': 'tucoytico.kommo.com',
        'campos': {'cliente': 'F ult msj cliente', 'asesor': 'F ult msj asesor'},
        'tz_offset': -6,
        'horario': (8, 18),
        'dias_laborables': [0, 1, 2, 3, 4, 5],
        'franjas': [
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#10b981', 'tag': 'Ideal'},
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
            {'label': '0 – 5 min',   'max_seg': 300,  'color': '#10b981', 'tag': 'Ideal'},
            {'label': '5 – 15 min',  'max_seg': 900,  'color': '#f59e0b', 'tag': 'Bueno'},
            {'label': '15 – 30 min', 'max_seg': 1800, 'color': '#f97316', 'tag': 'Regular'},
            {'label': '+30 min',     'max_seg': None,  'color': '#ef4444', 'tag': 'Crítico'},
        ]
    },
}

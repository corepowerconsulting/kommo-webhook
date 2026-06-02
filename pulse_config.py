PULSE_CONFIG = {
    'corepowerconsulting': {
        'nombre': 'Core Power Consulting',
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

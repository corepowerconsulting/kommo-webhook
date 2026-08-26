"""Todos los colores del dashboard, en un solo lugar.

Antes vivian en tres archivos —las franjas en pulse_config, los asesores en
kommo.py, y los grises sueltos dentro del template— asi que no habia forma de
ver la paleta entera ni de saber si un color nuevo chocaba con los que ya
estaban. Dos veces se eligio un color a ojo y dos veces salio mal.

De aca sale todo: kommo.py lo importa, pulse_config arma las franjas con esto,
y el template lo recibe y lo publica como variables CSS y como objeto de JS.
Un color nuevo se agrega ACA y aparece en los tres lados.

Las decisiones estan medidas, no elegidas a ojo. Los scripts que las midieron
comprueban tres cosas:

  contraste WCAG      >= 4.5:1 para texto, >= 3:1 para barras y lineas
  distancia entre si  delta E sobre CIELAB, para que no se confundan
  daltonismo          simulacion de protanopia y deuteranopia
"""

# --- Estructura: la regla de los tres colores --------------------------------
# Fondo aparte, todo lo demas se apoya en tres: tinta para el texto, gris para
# lo secundario, y un acento para lo que hay que mirar.
TINTA        = '#0f172a'   # texto principal y la linea del equipo
TEXTO        = '#334155'   # texto normal
APAGADO      = '#64748b'   # secundario. 4.76:1 sobre blanco, el minimo para texto
TENUE        = '#94a3b8'   # SOLO iconos decorativos: 2.56:1, no alcanza para texto
LINEA        = '#e2e8f0'   # bordes
CARRIL       = '#f1f5f9'   # fondo de barras y grillas
FONDO_SUAVE  = '#f8fafc'   # hover y subrecuadros

MARCA        = '#2563eb'   # un solo azul, botones y acentos
MARCA_OSCURA = '#1d4ed8'   # su hover
FOCO         = '#60a5fa'   # borde del control enfocado
ALERTA       = '#dc2626'   # rojo: lo unico que exige accion
OK           = '#16a34a'   # verde: el objetivo cumplido

# --- Franjas: el semaforo -----------------------------------------------------
# Convencion cultural verde -> amarillo -> naranja -> rojo. Se respeta aunque el
# amarillo quede en 1.92:1 sobre blanco: son arcos y cuadraditos rellenos, no
# letras, y un amarillo con contraste suficiente deja de leerse amarillo.
#
# En deuteranopia 'Bueno' y 'Regular' quedan a 11.7 y se confunden. No se
# cambia el semaforo por eso: cada franja lleva su etiqueta de texto al lado,
# que es la segunda señal.
FRANJAS = ['#22c55e', '#eab308', '#f97316', '#dc2626']

# Version tenue de cada franja, para el fondo de las etiquetas Ideal / Bueno /
# Regular / Critico. El color pleno detras de texto chico no se puede leer —el
# amarillo pleno da 1.92:1— asi que la etiqueta usa fondo claro y texto oscuro
# de la MISMA familia: conserva el significado y pasa de 7:1.
FRANJAS_FONDO = ['#dcfce7', '#fef3c7', '#ffedd5', '#fee2e2']
FRANJAS_TEXTO = ['#166534', '#92400e', '#9a3412', '#991b1b']

# --- Asesores: identifican personas, no calidad -------------------------------
# Sin verde ni rojo: en esta pantalla esos dos ya significan "bien" y "mal", y
# un asesor pintado de rojo pareceria que atiende mal.
#
# No elegidos a ojo: es la combinacion de 6 sobre 17 candidatos que maximiza la
# distancia del par mas parecido (53.6). La version anterior tenia dos a 25.9 y
# se confundian en pantalla.
#
# En escala de grises el par mas parecido cae a 2.4, o sea que impresos en
# blanco y negro dos asesores se ven iguales. Por eso cada linea lleva ademas
# su patron de guion y su forma de punto.
ASESORES = ['#1e3a8a', '#0891b2', '#78350f', '#4d7c0f', '#db2777', '#7c3aed']

# Gris para "Otros": no es una persona, no compite por atencion con las que si
# lo son. Es el uso que la bibliografia reserva al gris —miscelaneos, sin
# datos, no sabe— y por eso no se usa para ninguna categoria real.
OTROS = '#cbd5e1'


def para_css():
    """La paleta como variables CSS, para inyectar en el template."""
    pares = [
        ('tinta', TINTA), ('texto', TEXTO), ('apagado', APAGADO),
        ('tenue', TENUE), ('linea', LINEA), ('carril', CARRIL),
        ('fondo-suave', FONDO_SUAVE), ('marca', MARCA),
        ('marca-oscura', MARCA_OSCURA), ('foco', FOCO),
        ('alerta', ALERTA), ('ok', OK), ('otros', OTROS),
    ]
    # Las etiquetas van indexadas para que se lea que son las cuatro franjas
    # en orden y no cuatro colores sueltos que casualmente son verde a rojo.
    #
    # Van los tres tonos de cada una: el pleno para bordes y barras, el fondo
    # claro para pintar una tarjeta entera, y el texto oscuro para escribir
    # encima de ese fondo.
    for i, (pleno, f, t) in enumerate(zip(FRANJAS, FRANJAS_FONDO, FRANJAS_TEXTO)):
        pares += [(f'franja-{i}', pleno),
                  (f'franja-{i}-fondo', f), (f'franja-{i}-texto', t)]
    return '\n'.join(f'      --{n}: {v};' for n, v in pares)


def para_js():
    """La paleta como diccionario, para que el JS no repita hex sueltos."""
    return {
        'tinta': TINTA, 'texto': TEXTO, 'apagado': APAGADO, 'tenue': TENUE,
        'linea': LINEA, 'carril': CARRIL, 'marca': MARCA,
        'alerta': ALERTA, 'ok': OK, 'otros': OTROS,
    }

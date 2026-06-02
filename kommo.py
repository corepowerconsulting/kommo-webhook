from flask import Flask, request, jsonify, render_template
import json
from datetime import datetime, timedelta
import calendar
from collections import defaultdict
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from pulse_config import PULSE_CONFIG

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no está configurada")

# ========================
# BASE DE DATOS
# ========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS eventos (
            id SERIAL PRIMARY KEY,
            subdomain TEXT,
            tipo_evento TEXT,
            lead_id TEXT,
            timestamp BIGINT,
            raw_data TEXT,
            capturado_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS tiempos_respuesta (
            id SERIAL PRIMARY KEY,
            subdomain TEXT,
            lead_id TEXT,
            f_ult_msj_cliente BIGINT,
            f_ult_msj_asesor BIGINT,
            tiempo_respuesta_seg INTEGER,
            capturado_at TEXT,
            UNIQUE (subdomain, lead_id, f_ult_msj_asesor)
        )
    ''')
    conn.commit()
    conn.close()
    print("✅ Base de datos lista")

init_db()

# ========================
# HELPERS
# ========================
def get_custom_field(data, prefix, field_name):
    for i in range(20):
        if data.get(f'{prefix}[custom_fields][{i}][name]') == field_name:
            val = data.get(f'{prefix}[custom_fields][{i}][values][0]')
            try:
                return int(val)
            except (TypeError, ValueError):
                return None
    return None

# ========================
# GUARDAR EVENTO
# ========================
def guardar_evento(subdomain, tipo_evento, lead_id, timestamp, data):
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO eventos (subdomain, tipo_evento, lead_id, timestamp, raw_data, capturado_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            subdomain,
            tipo_evento,
            lead_id,
            int(timestamp) if timestamp else None,
            json.dumps(data),
            datetime.now().isoformat()
        ))
        conn.commit()
        print(f"💾 {subdomain} | {tipo_evento} | lead: {lead_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

def guardar_tiempo_respuesta(subdomain, lead_id, f_cliente, f_asesor):
    tiempo_seg = f_asesor - f_cliente
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO tiempos_respuesta
                (subdomain, lead_id, f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, capturado_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (lead_id, f_ult_msj_asesor) DO NOTHING
        ''', (subdomain, lead_id, f_cliente, f_asesor, tiempo_seg, datetime.now().isoformat()))
        conn.commit()
        print(f"⏱️  {subdomain} | lead {lead_id} | asesor respondió en {tiempo_seg // 60}min {tiempo_seg % 60}s")
    except Exception as e:
        print(f"❌ Error tiempo respuesta: {e}")
    finally:
        conn.close()

# ========================
# WEBHOOK
# ========================
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.form.to_dict()
        subdomain = data.get('account[subdomain]', 'desconocido')

        if any(k.startswith('message[add]') for k in data.keys()):
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='mensaje',
                lead_id=data.get('message[add][0][element_id]'),
                timestamp=data.get('message[add][0][created_at]'),
                data=data
            )

        elif any(k.startswith('leads[update]') for k in data.keys()):
            lead_id = data.get('leads[update][0][id]')
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_update',
                lead_id=lead_id,
                timestamp=data.get('leads[update][0][updated_at]'),
                data=data
            )
            # Calcular tiempo de respuesta del asesor
            prefix = 'leads[update][0]'
            f_cliente = get_custom_field(data, prefix, 'F Ult msj cliente')
            f_asesor = get_custom_field(data, prefix, 'F ult msj asesor')
            if f_cliente and f_asesor and f_asesor > f_cliente:
                guardar_tiempo_respuesta(subdomain, lead_id, f_cliente, f_asesor)

        elif any(k.startswith('leads[status]') for k in data.keys()):
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_status',
                lead_id=data.get('leads[status][0][id]'),
                timestamp=data.get('leads[status][0][updated_at]'),
                data=data
            )

        elif any(k.startswith('leads[responsible]') for k in data.keys()):
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_responsible',
                lead_id=data.get('leads[responsible][0][id]'),
                timestamp=data.get('leads[responsible][0][updated_at]'),
                data=data
            )

        else:
            print(f"⚠️  Ignorado | {subdomain} | {list(data.keys())[:2]}")

        return jsonify({'status': 'ok'}), 200

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return jsonify({'status': 'error'}), 200

# ========================
# ENDPOINTS
# ========================
@app.route('/')
def home():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute('SELECT subdomain, tipo_evento, COUNT(*) FROM eventos GROUP BY subdomain, tipo_evento')
        rows = c.fetchall()
        c.execute('SELECT COUNT(*) FROM eventos')
        total = c.fetchone()[0]
        resumen = {}
        for subdomain, tipo, count in rows:
            if subdomain not in resumen:
                resumen[subdomain] = {}
            resumen[subdomain][tipo] = count
        return jsonify({'total': total, 'por_cliente': resumen})
    finally:
        conn.close()

@app.route('/data')
def ver_datos():
    subdomain = request.args.get('subdomain')
    tipo = request.args.get('tipo')
    lead = request.args.get('lead_id')

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        query = 'SELECT * FROM eventos WHERE 1=1'
        params = []
        if subdomain:
            query += ' AND subdomain = %s'
            params.append(subdomain)
        if tipo:
            query += ' AND tipo_evento = %s'
            params.append(tipo)
        if lead:
            query += ' AND lead_id = %s'
            params.append(lead)
        query += ' ORDER BY timestamp DESC LIMIT 50'
        c.execute(query, params)
        rows = [dict(r) for r in c.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()

@app.route('/respuestas')
def ver_respuestas():
    subdomain = request.args.get('subdomain')
    lead = request.args.get('lead_id')

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)

        # Resumen agregado por subdomain
        c.execute('''
            SELECT
                subdomain,
                COUNT(*) AS total_mediciones,
                ROUND(AVG(tiempo_respuesta_seg))       AS promedio_seg,
                ROUND(AVG(tiempo_respuesta_seg) / 60.0, 1) AS promedio_min,
                MIN(tiempo_respuesta_seg)              AS minimo_seg,
                MAX(tiempo_respuesta_seg)              AS maximo_seg
            FROM tiempos_respuesta
            GROUP BY subdomain
        ''')
        resumen = [dict(r) for r in c.fetchall()]

        # Detalle con filtros opcionales
        query = 'SELECT * FROM tiempos_respuesta WHERE 1=1'
        params = []
        if subdomain:
            query += ' AND subdomain = %s'
            params.append(subdomain)
        if lead:
            query += ' AND lead_id = %s'
            params.append(lead)
        query += ' ORDER BY f_ult_msj_asesor DESC LIMIT 100'
        c.execute(query, params)
        detalle = [dict(r) for r in c.fetchall()]

        return jsonify({'resumen': resumen, 'detalle': detalle})
    finally:
        conn.close()

@app.route('/backfill')
def backfill():
    offset    = int(request.args.get('offset', 0))
    subdomain = request.args.get('subdomain')
    limit     = 300

    conn = get_conn()
    try:
        read_c  = conn.cursor(cursor_factory=RealDictCursor)
        write_c = conn.cursor()

        if subdomain:
            read_c.execute(
                "SELECT subdomain, lead_id, raw_data FROM eventos WHERE tipo_evento = 'lead_update' AND subdomain = %s ORDER BY id DESC LIMIT %s OFFSET %s",
                (subdomain, limit, offset)
            )
        else:
            read_c.execute(
                "SELECT subdomain, lead_id, raw_data FROM eventos WHERE tipo_evento = 'lead_update' ORDER BY id DESC LIMIT %s OFFSET %s",
                (limit, offset)
            )
        rows = read_c.fetchall()

        procesados = 0
        insertados = 0
        errores    = 0

        for row in rows:
            procesados += 1
            try:
                if not row['raw_data']:
                    continue
                data      = json.loads(row['raw_data'])
                prefix    = 'leads[update][0]'
                f_cliente = get_custom_field(data, prefix, 'F Ult msj cliente')
                f_asesor  = get_custom_field(data, prefix, 'F ult msj asesor')

                if f_cliente and f_asesor and f_asesor > f_cliente:
                    write_c.execute('''
                        INSERT INTO tiempos_respuesta
                            (subdomain, lead_id, f_ult_msj_cliente, f_ult_msj_asesor, tiempo_respuesta_seg, capturado_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (subdomain, lead_id, f_ult_msj_asesor) DO NOTHING
                    ''', (
                        row['subdomain'], row['lead_id'],
                        f_cliente, f_asesor, f_asesor - f_cliente,
                        datetime.now().isoformat()
                    ))
                    if write_c.rowcount > 0:
                        insertados += 1
            except Exception as e:
                errores += 1
                print(f"⚠️ Backfill skip lead {row.get('lead_id')}: {e}")
                continue

        conn.commit()
        hay_mas = len(rows) == limit
        return jsonify({
            'offset': offset,
            'procesados': procesados,
            'insertados': insertados,
            'errores': errores,
            'siguiente': f'/backfill?offset={offset + limit}' if hay_mas else None,
            'listo': not hay_mas
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ========================
# PULSE
# ========================
DIAS_LABEL = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']

def _ts_to_local(ts, tz_offset):
    return datetime.utcfromtimestamp(ts) + timedelta(hours=tz_offset)

def _local_date_to_ts(date_str, tz_offset):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    dt_utc = dt - timedelta(hours=tz_offset)
    return calendar.timegm(dt_utc.timetuple())

def _fmt_horario(h_ini, h_fin):
    def fh(h):
        if h == 0: return '12:00 AM'
        if h < 12: return f'{h}:00 AM'
        if h == 12: return '12:00 PM'
        return f'{h - 12}:00 PM'
    return f'{fh(h_ini)} – {fh(h_fin)}'

def _get_franja_idx(seg, franjas):
    for i, f in enumerate(franjas):
        if f['max_seg'] is None or seg < f['max_seg']:
            return i
    return len(franjas) - 1

def _calc_metricas(registros, franjas):
    if not registros:
        return {
            'total': 0,
            'promedio_seg': 0,
            'distribucion': [
                {'label': f['label'], 'tag': f['tag'], 'count': 0, 'pct': 0.0, 'color': f['color']}
                for f in franjas
            ]
        }
    total = len(registros)
    promedio_seg = round(sum(r['tiempo_respuesta_seg'] for r in registros) / total)
    counts = [0] * len(franjas)
    for r in registros:
        counts[_get_franja_idx(r['tiempo_respuesta_seg'], franjas)] += 1
    distribucion = [
        {'label': franjas[i]['label'], 'tag': franjas[i]['tag'],
         'count': counts[i], 'pct': round(counts[i] / total * 100, 1), 'color': franjas[i]['color']}
        for i in range(len(franjas))
    ]
    return {'total': total, 'promedio_seg': promedio_seg, 'distribucion': distribucion}

def _fmt_row(r, tz_offset):
    seg = r['tiempo_respuesta_seg']
    local_dt = _ts_to_local(r['f_ult_msj_cliente'], tz_offset)
    if seg >= 3600:
        fmt = f"{seg // 3600}h {(seg % 3600) // 60}m"
    elif seg >= 60:
        fmt = f"{seg // 60}m {seg % 60}s"
    else:
        fmt = f"{seg}s"
    return {
        'lead_id': r['lead_id'],
        'seg': seg,
        'fmt': fmt,
        'fecha': local_dt.strftime('%d/%m %H:%M'),
    }

@app.route('/pulse')
def pulse():
    subdomain = request.args.get('subdomain', '')
    return render_template('pulse.html', subdomain=subdomain)

@app.route('/pulse/data')
def pulse_data():
    subdomain = request.args.get('subdomain', '').strip()
    if not subdomain or subdomain not in PULSE_CONFIG:
        return jsonify({'error': f'Subdomain "{subdomain}" no configurado en PULSE'}), 400

    cfg = PULSE_CONFIG[subdomain]
    tz_offset = cfg['tz_offset']
    h_ini, h_fin = cfg['horario']
    dias_lab = set(cfg['dias_laborables'])
    franjas = cfg['franjas']

    desde_str = request.args.get('desde')
    hasta_str = request.args.get('hasta')

    conn = get_conn()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)

        query = '''
            SELECT lead_id, f_ult_msj_cliente, tiempo_respuesta_seg
            FROM tiempos_respuesta
            WHERE subdomain = %s AND tiempo_respuesta_seg > 0 AND f_ult_msj_cliente IS NOT NULL
        '''
        params = [subdomain]

        if desde_str:
            query += ' AND f_ult_msj_cliente >= %s'
            params.append(_local_date_to_ts(desde_str, tz_offset))
        if hasta_str:
            query += ' AND f_ult_msj_cliente <= %s'
            params.append(_local_date_to_ts(hasta_str, tz_offset) + 86399)

        c.execute(query, params)
        rows = c.fetchall()
    except Exception as e:
        print(f'❌ PULSE /data error: {e}')
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

    try:

        laboral, fuera = [], []
        for row in rows:
            local_dt = _ts_to_local(row['f_ult_msj_cliente'], tz_offset)
            if local_dt.weekday() in dias_lab and h_ini <= local_dt.hour < h_fin:
                laboral.append(row)
            else:
                fuera.append(row)

        todos = laboral + fuera
        top_lentos = sorted(todos, key=lambda r: r['tiempo_respuesta_seg'], reverse=True)[:10]
        top_rapidos = sorted(todos, key=lambda r: r['tiempo_respuesta_seg'])[:10]

        daily = defaultdict(list)
        for r in todos:
            dia = _ts_to_local(r['f_ult_msj_cliente'], tz_offset).strftime('%Y-%m-%d')
            daily[dia].append(r['tiempo_respuesta_seg'])
        tendencia = [
            {'fecha': dia, 'promedio_min': round(sum(v) / len(v) / 60, 1), 'total': len(v)}
            for dia in sorted(daily)[-30:]
        ]

        return jsonify({
            'config': {
                'nombre': cfg['nombre'],
                'horario_fmt': _fmt_horario(h_ini, h_fin),
                'dias_fmt': ' · '.join(DIAS_LABEL[d] for d in sorted(dias_lab)),
                'franjas': franjas,
            },
            'laboral': _calc_metricas(laboral, franjas),
            'fuera_horario': _calc_metricas(fuera, franjas),
            'top_lentos': [_fmt_row(r, tz_offset) for r in top_lentos],
            'top_rapidos': [_fmt_row(r, tz_offset) for r in top_rapidos],
            'tendencia': tendencia,
        })
    except Exception as e:
        print(f'❌ PULSE /data procesamiento: {e}')
        return jsonify({'error': str(e)}), 500

# ========================
# MAIN
# ========================
if __name__ == '__main__':
    print("🚀 Servidor corriendo en puerto 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)

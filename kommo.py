from flask import Flask, request, jsonify
import json
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')

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
    conn.commit()
    conn.close()
    print("✅ Base de datos lista")

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
            timestamp,
            json.dumps(data),
            datetime.now().isoformat()
        ))
        conn.commit()
        print(f"💾 {subdomain} | {tipo_evento} | lead: {lead_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
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
            guardar_evento(
                subdomain=subdomain,
                tipo_evento='lead_update',
                lead_id=data.get('leads[update][0][id]'),
                timestamp=data.get('leads[update][0][updated_at]'),
                data=data
            )

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
    c = conn.cursor()
    c.execute('SELECT subdomain, tipo_evento, COUNT(*) FROM eventos GROUP BY subdomain, tipo_evento')
    rows = c.fetchall()
    c.execute('SELECT COUNT(*) FROM eventos')
    total = c.fetchone()[0]
    conn.close()
    resumen = {}
    for subdomain, tipo, count in rows:
        if subdomain not in resumen:
            resumen[subdomain] = {}
        resumen[subdomain][tipo] = count
    return jsonify({'total': total, 'por_cliente': resumen})

@app.route('/data')
def ver_datos():
    subdomain = request.args.get('subdomain')
    tipo = request.args.get('tipo')
    lead = request.args.get('lead_id')

    conn = get_conn()
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
    conn.close()
    return jsonify(rows)

# ========================
# MAIN
# ========================
if __name__ == '__main__':
    init_db()
    print("🚀 Servidor corriendo en puerto 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
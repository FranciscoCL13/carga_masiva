from flask import Flask, request, jsonify, render_template
import pandas as pd
import numpy as np
import requests
import base64

app = Flask(__name__)

# -------------------- Configuración KIE --------------------
KIE_BASE = "http://localhost:8080/kie-server/services/rest/server"
CONTAINER_ID = "Publica_In_Out_1.0.0-SNAPSHOT"
PROCESS_ID = "Publica_In_Out.Publica"
USERNAME = "wbadmin"
PASSWORD = "wbadmin"

auth_header = {
    "Content-Type": "application/json",
    "Authorization": "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
}

# -------------------- Funciones auxiliares --------------------
def make_serializable(val):
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    if isinstance(val, np.datetime64):
        return pd.to_datetime(val).isoformat()
    return val

def start_process(variables):
    """Crea una instancia del proceso con las variables"""
    url = f"{KIE_BASE}/containers/{CONTAINER_ID}/processes/{PROCESS_ID}/instances"
    print(f"🔹 Enviando variables al proceso: {variables}")
    resp = requests.post(url, headers=auth_header, json=variables)
    resp.raise_for_status()
    pid = int(resp.text)
    print(f"✅ Proceso creado con processInstanceId: {pid}")
    return pid

def complete_task(task_id, variables):
    url = f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/completed"
    payload = {
        "user": USERNAME,        # nombre del usuario que completa la tarea
        "task-output": variables # variables para la tarea
    }
    resp = requests.put(url, headers=auth_header, json=payload)
    resp.raise_for_status()
    print(f"✅ Tarea {task_id} completada")

# -------------------- Rutas Flask --------------------
@app.route("/cargar_excel", methods=["POST"])
def cargar_excel():
    if "file" not in request.files:
        return jsonify({"error": "No se envió ningún archivo"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Archivo vacío"}), 400

    try:
        #LEE LA PRIMER HOJA DEL EXCEL
        df = pd.read_excel(file).replace([np.inf, -np.inf], np.nan)

        # ⬅ Aquí forzamos la columna a tipo datetime
        df['fec_oficio_sol'] = pd.to_datetime(df['fec_oficio_sol'], errors='coerce')

        filas_raw = df.to_dict(orient="records")
        filas = [{k: make_serializable(v) for k, v in row.items()} for row in filas_raw]

        resultados = []
        for idx, fila in enumerate(filas, start=1):
            print(f"\n=== Procesando fila {idx}: {fila} ===")

            # Filtrar solo variables con valor
            variables = {k: v for k, v in fila.items() if v is not None}

            # Crear instancia de proceso
            pid = start_process(variables)

            # Obtener la tarea activa asociada a esta instancia
            url_tasks = f"{KIE_BASE}/queries/tasks/instances/pot-owners?processInstanceId={pid}"
            resp_tasks = requests.get(url_tasks, headers=auth_header)
            resp_tasks.raise_for_status()
            tasks = resp_tasks.json().get('task-summary', [])

            if tasks:
                task_id = tasks[0]['task-id']  # tomamos la primera tarea
                
                # 1️⃣ Claim la tarea
                requests.put(f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/claimed",
                            headers=auth_header, json={"user": USERNAME}).raise_for_status()

                # 2️⃣ Start la tarea
                requests.put(f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/started",
                            headers=auth_header, json={"user": USERNAME}).raise_for_status()

                # 3️⃣ Complete la tarea
                requests.put(f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/completed",
                            headers=auth_header, json={"user": USERNAME, "task-output": variables}).raise_for_status()


                
                
                # complete_task(task_id, variables)  # completamos la tarea con las mismas variables

            resultados.append({
                "fila": fila,
                "status": "instance_created",
                "process_instance_id": pid
            })

        return jsonify({"processed": len(filas), "results": resultados})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def index():
    return render_template("index.html")

# -------------------- Ejecutar Flask --------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)

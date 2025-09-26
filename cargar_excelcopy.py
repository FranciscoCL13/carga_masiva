from flask import Flask, request, jsonify, render_template
import pandas as pd
import numpy as np
import requests
import base64
import time

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

# NodeIds de tareas humanas
NODE_SEDATU = "_E973B0C7-7DB3-4F29-9C4F-78F6F00E8D65"
NODE_INDAABIN = "_C2BB9A4D-DBB4-409D-B1CB-83E82D3C392E"

# -------------------- Funciones auxiliares --------------------
def make_serializable(val):
    """Convierte valores a tipos JSON serializables"""
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
    resp = requests.post(url, headers=auth_header, json=variables)
    resp.raise_for_status()
    pid = int(resp.text)
    print(f"🟢 Proceso iniciado con PID: {pid}")
    return pid

def claim_start_complete(task_id, variables):
    """Hace claim + start + complete de una tarea humana"""
    requests.put(f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/claimed",
                 headers=auth_header, json={"user": USERNAME}).raise_for_status()
    requests.put(f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/started",
                 headers=auth_header, json={"user": USERNAME}).raise_for_status()
    requests.put(f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/completed",
                 headers=auth_header, json={"user": USERNAME, "task-output": variables}).raise_for_status()
    print(f"✅ Tarea {task_id} completada")

def wait_for_task(process_instance_id, retries=10, delay=1):
    """Espera a que aparezca la última tarea humana del proceso"""
    url_tasks = f"{KIE_BASE}/queries/tasks/instances/pot-owners?processInstanceId={process_instance_id}"
    for _ in range(retries):
        resp = requests.get(url_tasks, headers=auth_header)
        resp.raise_for_status()
        tasks = resp.json().get('task-summary', [])
        if tasks:
            return tasks[-1]  # última tarea creada
        time.sleep(delay)
    return None

def trigger_and_complete(process_instance_id, node_id, variables, max_wait=10):
    """
    Dispara un nodo, espera a que la tarea asociada aparezca y la completa.
    
    :param process_instance_id: ID de la instancia de proceso
    :param node_id: NodeId de la tarea humana
    :param variables: Diccionario con variables para completar la tarea
    :param max_wait: Tiempo máximo en segundos para esperar la tarea
    :return: task_id completada o None si falla
    """
    import time

    # ---------------- Lanzar trigger ----------------
    url_trigger = f"{KIE_BASE}/containers/{CONTAINER_ID}/processes/instances/{process_instance_id}/nodeInstances"
    payload = {"nodeId": node_id}
    r = requests.post(url_trigger, headers=auth_header, json=payload)
    r.raise_for_status()

    # ---------------- Esperar la tarea correcta ----------------
    url_tasks = f"{KIE_BASE}/queries/tasks/instances/pot-owners?processInstanceId={process_instance_id}"
    task_id = None
    for _ in range(max_wait):
        resp_tasks = requests.get(url_tasks, headers=auth_header)
        resp_tasks.raise_for_status()
        tasks = resp_tasks.json().get('task-summary', [])
        # Filtrar por NodeId y por usuario asignado
        for t in tasks:
            if t.get('task-node-instance-id') == node_id or t.get('actual-owner') == USERNAME:
                task_id = t['task-id']
                break
        if task_id:
            break
        time.sleep(1)

    if not task_id:
        print(f"⚠️ No se encontró tarea para nodo {node_id} después de {max_wait}s")
        return None

    # ---------------- Claim + Start + Complete ----------------
    try:
        # Claim
        requests.put(
            f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/claimed",
            headers=auth_header, json={"user": USERNAME}
        ).raise_for_status()
        # Start
        requests.put(
            f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/started",
            headers=auth_header, json={"user": USERNAME}
        ).raise_for_status()
        # Complete
        requests.put(
            f"{KIE_BASE}/containers/{CONTAINER_ID}/tasks/{task_id}/states/completed",
            headers=auth_header, json={"user": USERNAME, "task-output": variables}
        ).raise_for_status()
        print(f"✅ Tarea {task_id} completada")
        return task_id
    except requests.exceptions.HTTPError as e:
        print(f"⚠️ Error completando tarea {task_id}: {e}")
        return None


# -------------------- Rutas Flask --------------------
@app.route("/cargar_excel", methods=["POST"])
def cargar_excel():
    if "file" not in request.files:
        return jsonify({"error": "No se envió ningún archivo"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Archivo vacío"}), 400

    try:
        # Leer todas las hojas del Excel
        sheets = pd.read_excel(file, sheet_name=None)
        resultados = {}

        # ---------------- Hoja1 → Primera tarea ----------------
        if "Hoja1" in sheets:
            df1 = sheets["Hoja1"].replace([np.inf, -np.inf], np.nan)
            fila = df1.to_dict(orient="records")[0]
            variables1 = {k: make_serializable(v) for k, v in fila.items() if v is not None}

            pid = start_process(variables1)

            # Completar primera tarea
            url_tasks = f"{KIE_BASE}/queries/tasks/instances/pot-owners?processInstanceId={pid}"
            resp_tasks = requests.get(url_tasks, headers=auth_header)
            resp_tasks.raise_for_status()
            tasks = resp_tasks.json().get('task-summary', [])

            if tasks:
                task_id = tasks[0]['task-id']
                claim_start_complete(task_id, variables1)

            resultados["Hoja1"] = {"processInstanceId": pid, "status": "completed"}
        else:
            return jsonify({"error": "El Excel no tiene Hoja1"}), 400

        # ---------------- Hoja2 → Sedatu ----------------
        if "Hoja2" in sheets:
            df2 = sheets["Hoja2"].replace([np.inf, -np.inf], np.nan)
            fila2 = df2.to_dict(orient="records")[0]
            variables2 = {k: make_serializable(v) for k, v in fila2.items() if v is not None}

            task_id2 = trigger_and_complete(pid, NODE_SEDATU, variables2)
            resultados["Hoja2"] = {"taskId": task_id2, "status": "completed" if task_id2 else "not found"}

        # ---------------- Hoja3 → Indaabin ----------------
        if "Hoja3" in sheets:
            df3 = sheets["Hoja3"].replace([np.inf, -np.inf], np.nan)
            fila3 = df3.to_dict(orient="records")[0]
            variables3 = {k: make_serializable(v) for k, v in fila3.items() if v is not None}

            task_id3 = trigger_and_complete(pid, NODE_INDAABIN, variables3)
            resultados["Hoja3"] = {"taskId": task_id3, "status": "completed" if task_id3 else "not found"}

        return jsonify(resultados)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def index():
    return render_template("index.html")

# -------------------- Ejecutar Flask --------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)

from flask import Flask, request, jsonify, render_template
import pandas as pd
import time
import requests
from requests.auth import HTTPBasicAuth
from lxml import etree

# ---------------- CONFIGURACIÓN ----------------
KIE_SERVER = "http://localhost:8080/kie-server/services/rest/server"
CONTAINER_ID = "Publica_In_Out_1.0.0-SNAPSHOT"
PROCESS_ID = "Publica_In_Out.Publica"
USER = "wbadmin"
PASSWORD = "wbadmin"

app = Flask(__name__)

# ---------------- FUNCIONES ----------------
def start_process():
    """Inicia un proceso y devuelve su process instance ID"""
    url = f"{KIE_SERVER}/containers/{CONTAINER_ID}/processes/{PROCESS_ID}/instances"
    resp = requests.post(url, json={}, auth=HTTPBasicAuth(USER, PASSWORD))
    resp.raise_for_status()
    pid = resp.text.strip()
    print(f"🟢 Proceso iniciado con PID: {pid}")
    return pid

def parse_tasks_xml(tasks_xml, pid):
    """Parsea el XML de tareas usando lxml y maneja namespaces automáticamente"""
    root = etree.fromstring(tasks_xml.encode())
    tasks = []
    # buscar task-summary ignorando namespaces
    for ts in root.xpath("//*[local-name()='task-summary']"):
        task_proc_id = int(ts.xpath("*[local-name()='task-proc-inst-id']/text()")[0])
        if task_proc_id == int(pid):
            task_id = int(ts.xpath("*[local-name()='task-id']/text()")[0])
            tasks.append({"task-id": task_id, "task-proc-inst-id": task_proc_id})
    return pd.DataFrame(tasks)

def get_tasks(pid):
    """Obtiene todas las tareas de la instancia"""
    url = f"{KIE_SERVER}/queries/tasks/instances/pot-owners?containerId={CONTAINER_ID}"
    for _ in range(20):  # esperar hasta 20 ciclos de 1s
        resp = requests.get(url, auth=HTTPBasicAuth(USER, PASSWORD))
        resp.raise_for_status()
        tasks_xml = resp.text
        try:
            tasks = parse_tasks_xml(tasks_xml, pid)
            if not tasks.empty:
                return tasks
        except Exception as e:
            print(f"⚠️ Error leyendo XML de tareas: {e}")
        time.sleep(1)
    return pd.DataFrame()

def complete_task(task_id, data={}):
    """Completa una tarea con los datos proporcionados"""
    url = f"{KIE_SERVER}/containers/{CONTAINER_ID}/tasks/{task_id}/states/completed"
    resp = requests.put(url, json=data, auth=HTTPBasicAuth(USER, PASSWORD))
    resp.raise_for_status()
    print(f"✅ Tarea {task_id} completada")

# ---------------- RUTAS FLASK ----------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/cargar_excel", methods=["POST"])
def cargar_excel():
    if "file" not in request.files:
        return jsonify({"error": "No se envió ningún archivo"}), 400

    file = request.files["file"]
    try:
        excel_data = pd.read_excel(file, sheet_name=None)  # leer todas las hojas
    except Exception as e:
        return jsonify({"error": f"No se pudo leer el Excel: {e}"}), 400

    resultados = []

    # número de filas de la primera hoja
    num_rows = len(next(iter(excel_data.values())))

    for index in range(num_rows):
        try:
            pid = start_process()
        except requests.exceptions.HTTPError as e:
            print(f"⚠️ Error iniciando proceso: {e}")
            resultados.append({"row": index, "status": "Error iniciar proceso"})
            continue

        tasks = get_tasks(pid)
        if tasks.empty:
            print(f"⚠️ No se encontraron tareas para PID {pid}")
            resultados.append({"row": index, "status": "No hay tareas"})
            continue

        # completar tareas en orden según hojas del Excel
        for i, (sheet_name, df) in enumerate(excel_data.items()):
            if i >= len(tasks):
                break
            task_id = tasks.iloc[i]["task-id"]
            row_data = df.iloc[index].to_dict()

            # Convertir Timestamps y NaN a valores JSON serializables
            for k, v in row_data.items():
                if pd.isna(v):
                    row_data[k] = None
                elif isinstance(v, pd.Timestamp):
                    row_data[k] = v.isoformat()

            try:
                complete_task(task_id, row_data)
            except requests.exceptions.HTTPError as e:
                print(f"⚠️ Error completando tarea {task_id}: {e}")
                continue

        resultados.append({"row": index, "status": "Proceso completado"})

    return jsonify(resultados)

# ---------------- EJECUCIÓN ----------------
if __name__ == "__main__":
    app.run(debug=True)

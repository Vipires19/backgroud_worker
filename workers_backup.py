import time
import os
import tempfile
import urllib.parse
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from pymongo import MongoClient

from services.pose_extractor import extract_landmarks_from_video
from services.pose_analyzer import analyze_poses
from services.video_generator import save_and_upload_comparative_video
from utils.helpers import generate_and_upload_pdf
from utils.openai_feedback import generate_feedback_via_openai
from utils.r2_utils import get_r2_client

# Carrega variáveis de ambiente
load_dotenv()

# --- Configurações ---
MONGO_USER = urllib.parse.quote_plus(os.getenv("MONGO_USER"))
MONGO_PASS = urllib.parse.quote_plus(os.getenv("MONGO_PASS"))
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gjkin5a.mongodb.net/?retryWrites=true&w=majority"

R2_KEY = os.getenv("R2_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
R2_BUCKET = os.getenv("R2_BUCKET")
API_KEY = os.getenv("OPENAI_API_KEY")

# --- Inicializações ---
client = MongoClient(MONGO_URI)
db = client.personalAI
queue = db.jobs_fila  # Coleção de fila
s3_client = get_r2_client(R2_KEY, R2_SECRET_KEY, ENDPOINT_URL)

print("[Worker] Iniciado. Monitorando fila...")

# --- Utilitários ---
def extract_key_from_url(url):
    if not url:
        print("[Erro] URL fornecida é None ou vazia!")
        return None
    try:
        parsed = urlparse(url)
        return os.path.basename(parsed.path) if parsed.path else None
    except Exception as e:
        print(f"[Erro] Falha ao extrair chave da URL: {e}")
        return None

# --- Processamento principal ---
def process_task(task):
    print(f"[Worker] Processando: {task.get('student', 'Desconhecido')}")

    try:
        ref_key = extract_key_from_url(task.get('ref_path'))
        exec_key = extract_key_from_url(task.get('exec_path'))

        if not ref_key or not exec_key:
            raise ValueError("Chaves de vídeo inválidas")

        # Verifica existência no R2
        s3_client.get_object(Bucket=R2_BUCKET, Key=ref_key)
        s3_client.get_object(Bucket=R2_BUCKET, Key=exec_key)

        # Baixa vídeos temporários
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as ref_temp:
            ref_temp.write(s3_client.get_object(Bucket=R2_BUCKET, Key=ref_key)['Body'].read())
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as exec_temp:
            exec_temp.write(s3_client.get_object(Bucket=R2_BUCKET, Key=exec_key)['Body'].read())

        # Processamento com MediaPipe
        frames_ref, landmarks_ref = extract_landmarks_from_video(ref_temp.name)
        frames_exec, landmarks_exec = extract_landmarks_from_video(exec_temp.name)
        if not frames_ref or not landmarks_ref or not frames_exec or not landmarks_exec:
            raise ValueError("Falha na extração de landmarks")

        insights, avg_error, avg_errors = analyze_poses(landmarks_ref, landmarks_exec)

        # Gera e envia vídeo
        video_key = f"comparativos/{task['student_name']}_comparativo.mp4"
        video_url = save_and_upload_comparative_video(
            frames_ref, landmarks_ref, frames_exec, landmarks_exec,
            upload_path=video_key,
            s3_client=s3_client,
            bucket_name=R2_BUCKET
        )

        # Gera e envia PDF
        pdf_key = f"relatorios/{task['student_name']}_relatorio.pdf"
        os.makedirs("temp", exist_ok=True)
        local_pdf = os.path.join("temp", f"{task['student_name']}_relatorio.pdf")
        full_feedback = generate_feedback_via_openai(avg_errors, API_KEY)
        pdf_url = generate_and_upload_pdf(
            task['student_name'], insights, avg_error, video_url,
            output_path_local=local_pdf,
            output_path_r2=pdf_key,
            s3_client=s3_client,
            bucket_name=R2_BUCKET,
            full_feedback=full_feedback
        )

        # Atualiza job no MongoDB
        queue.update_one(
            {"_id": task["_id"]},
            {
                "$set": {
                    "status": "done",
                    "video_url": video_url,
                    "report_url": pdf_url,
                    "feedback": full_feedback,
                    "processed_at": datetime.utcnow()
                }
            }
        )

        os.remove(ref_temp.name)
        os.remove(exec_temp.name)
        os.remove(local_pdf)

        print(f"[Worker] ✅ Finalizado: {task['student_name']}")

    except Exception as e:
        print(f"[Erro ao processar {task.get('student')}] {e}")
        queue.update_one(
            {"_id": task["_id"]},
            {"$set": {"status": "error", "error_message": str(e), "processed_at": datetime.utcnow()}}
        )

# --- Loop de monitoramento ---
while True:
    task = queue.find_one({"status": "pending"})
    if task:
        process_task(task)
    else:
        time.sleep(5)

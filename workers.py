# worker.py - Processador assíncrono de vídeos com MediaPipe

import time
import os
import tempfile
from pymongo import MongoClient
from services.pose_extractor import extract_landmarks_from_video
from services.pose_analyzer import analyze_poses
from services.video_generator import save_and_upload_comparative_video
from utils.helpers import generate_and_upload_pdf
from utils.openai_feedback import generate_feedback_via_openai
from utils.r2_utils import get_r2_client
import urllib.parse

# Configs sensíveis (em ambiente real, use dotenv/secrets)
from dotenv import load_dotenv
load_dotenv()

MONGO_USER = urllib.parse.quote_plus(os.getenv("MONGO_USER"))
MONGO_PASS = urllib.parse.quote_plus(os.getenv("MONGO_PASS"))
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gjkin5a.mongodb.net/?retryWrites=true&w=majority"

R2_KEY = os.getenv("R2_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

API_KEY = os.getenv("OPENAI_API_KEY")

client = MongoClient(MONGO_URI)
db = client.personalAI
queue = db.video_queue  # Coleção de fila

s3_client = get_r2_client(R2_KEY, R2_SECRET_KEY, ENDPOINT_URL)

print("[Worker] Iniciado. Monitorando fila...")

while True:
    task = queue.find_one_and_delete({})  # Pega e remove da fila
    if task:
        print(f"[Worker] Processando: {task['student_name']}")
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as ref_temp:
                ref_temp.write(s3_client.get_object(Bucket=BUCKET_NAME, Key=task['ref_video_key'])['Body'].read())

            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as exec_temp:
                exec_temp.write(s3_client.get_object(Bucket=BUCKET_NAME, Key=task['exec_video_key'])['Body'].read())

            frames_ref, landmarks_ref = extract_landmarks_from_video(ref_temp.name)
            frames_exec, landmarks_exec = extract_landmarks_from_video(exec_temp.name)
            insights, avg_error, avg_errors = analyze_poses(landmarks_ref, landmarks_exec)

            # Gera e sobe vídeo
            video_key = f"comparativos/{task['student_name']}_comparativo.mp4"
            video_url = save_and_upload_comparative_video(
                frames_ref, landmarks_ref, frames_exec, landmarks_exec,
                upload_path=video_key,
                s3_client=s3_client,
                bucket_name=BUCKET_NAME
            )

            # Gera e sobe PDF
            pdf_key = f"relatorios/{task['student_name']}_relatorio.pdf"
            local_pdf = os.path.join("temp", f"{task['student_name']}_relatorio.pdf")
            full_feedback = generate_feedback_via_openai(avg_errors, API_KEY)
            pdf_url = generate_and_upload_pdf(
                task['student_name'], insights, avg_error, video_url,
                output_path_local=local_pdf,
                output_path_r2=pdf_key,
                s3_client=s3_client,
                bucket_name=BUCKET_NAME,
                full_feedback=full_feedback
            )

            # Atualiza status
            db.results.insert_one({
                "student_name": task['student_name'],
                "user": task['user'],
                "video_url": video_url,
                "pdf_url": pdf_url,
                "feedback": full_feedback,
                "timestamp": time.time()
            })

            print(f"[Worker] Finalizado: {task['student_name']}")

            os.remove(ref_temp.name)
            os.remove(exec_temp.name)
            os.remove(local_pdf)

        except Exception as e:
            print(f"[Erro] {e}")
    else:
        time.sleep(5)  # Espera antes de verificar novamente

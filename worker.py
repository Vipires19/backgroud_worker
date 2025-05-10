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
from urllib.parse import urlparse
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de conexão com MongoDB
MONGO_USER = urllib.parse.quote_plus(os.getenv("MONGO_USER"))
MONGO_PASS = urllib.parse.quote_plus(os.getenv("MONGO_PASS"))
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gjkin5a.mongodb.net/?retryWrites=true&w=majority"

# Configuração do R2 (Cloudflare)
R2_KEY = os.getenv("R2_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

# Chave API OpenAI
API_KEY = os.getenv("OPENAI_API_KEY")

# Conecta ao banco de dados MongoDB
client = MongoClient(MONGO_URI)
db = client.personalAI
queue = db.jobs_fila  # Coleção de fila

# Inicializa o cliente R2
s3_client = get_r2_client(R2_KEY, R2_SECRET_KEY, ENDPOINT_URL)

print("[Worker] Iniciado. Monitorando fila...")

# Função para extrair o nome do arquivo da URL
def extract_key_from_url(url):
    if not url:
        print("[Erro] URL fornecida é None ou vazia!")
        return None
    try:
        parsed_url = urlparse(url)
        if parsed_url.path:
            return os.path.basename(parsed_url.path)
        else:
            print("[Erro] URL não tem caminho válido:", url)
            return None
    except Exception as e:
        print(f"[Erro] Falha ao parsear URL: {url}, erro: {e}")
        return None

# Função para processar a tarefa
def process_task(task):
    print(f"[Worker] Processando: {task.get('student_name', 'Desconhecido')}")

    try:
        # Obtém as chaves das URLs
        ref_path = task.get('ref_path')
        exec_path = task.get('exec_path')

        # Verifica se as URLs estão válidas
        if not ref_path or not exec_path:
            raise ValueError("ref_path ou exec_path ausente ou inválido.")
        
        ref_key = extract_key_from_url(ref_path)
        exec_key = extract_key_from_url(exec_path)

        # Se algum dos arquivos não foi encontrado, levanta um erro
        if not ref_key or not exec_key:
            raise ValueError("Não foi possível extrair a chave das URLs.")

        print(f"ref_key: {ref_key}, exec_key: {exec_key}")

        # Verifica se os arquivos existem no R2 antes de tentar fazer o download
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=ref_key)
            s3_client.head_object(Bucket=BUCKET_NAME, Key=exec_key)
        except Exception as e:
            raise ValueError(f"Erro ao verificar arquivos no R2: {e}")

        # Baixa os arquivos temporários a partir das URLs
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as ref_temp:
            ref_temp.write(s3_client.get_object(Bucket=BUCKET_NAME, Key=ref_key)['Body'].read())

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as exec_temp:
            exec_temp.write(s3_client.get_object(Bucket=BUCKET_NAME, Key=exec_key)['Body'].read())

        # Processa os vídeos para extrair landmarks
        frames_ref, landmarks_ref = extract_landmarks_from_video(ref_temp.name)
        frames_exec, landmarks_exec = extract_landmarks_from_video(exec_temp.name)

        # Verifica se os landmarks foram extraídos corretamente
        if not frames_ref or not landmarks_ref or not frames_exec or not landmarks_exec:
            raise ValueError("Falha ao extrair landmarks dos vídeos.")

        insights, avg_error, avg_errors = analyze_poses(landmarks_ref, landmarks_exec)

        # Gera e sobe o vídeo comparativo
        video_key = f"comparativos/{task['student_name']}_comparativo.mp4"
        video_url = save_and_upload_comparative_video(
            frames_ref, landmarks_ref, frames_exec, landmarks_exec,
            upload_path=video_key,
            s3_client=s3_client,
            bucket_name=BUCKET_NAME
        )

        # Gera e sobe o PDF
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

        # Armazena os resultados no banco de dados
        db.results.insert_one({
            "student_name": task['student_name'],
            "user": task['user'],
            "video_url": video_url,
            "pdf_url": pdf_url,
            "feedback": full_feedback,
            "timestamp": time.time()
        })

        # Remove os arquivos temporários
        os.remove(ref_temp.name)
        os.remove(exec_temp.name)
        os.remove(local_pdf)

        # Atualiza a fila de tarefas removendo a tarefa processada
        queue.delete_one({"_id": task["_id"]})

        print(f"[Worker] Finalizado: {task['student_name']}")

    except Exception as e:
        print(f"[Erro] {e}")

# Loop principal para monitorar e processar tarefas da fila
while True:
    task = queue.find_one({})  # Pega a próxima tarefa na fila

    if task:
        process_task(task)  # Processa a tarefa
    else:
        time.sleep(5)  # Espera antes de verificar novamente

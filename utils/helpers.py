from fpdf import FPDF
import os

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 14)
        self.cell(0, 10, 'Relatório de Análise de Exercício', 0, 1, 'C')
        self.ln(10)

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, body)
        self.ln()

def generate_pdf_report(student_name, insights, avg_error, video_url, output_path, full_feedback=None):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)  # Garante que o diretório existe

    pdf = PDF()
    pdf.add_page()

    pdf.chapter_title(f"Aluno: {student_name}")
    pdf.chapter_body(f"Erro médio total: {avg_error:.2f}°\n")

    pdf.chapter_title("Principais Correções:")
    for insight in insights[:5]:
        pdf.chapter_body(f"- {insight}")

    if full_feedback:
        pdf.chapter_title("Feedback Inteligente Personalizado:")
        pdf.chapter_body(full_feedback)

    if video_url:
        pdf.chapter_title("Link do vídeo comparativo:")
        pdf.chapter_body(video_url)
    else:
        pdf.chapter_title("⚠️ Vídeo não disponível")
        pdf.chapter_body("O vídeo não pôde ser gerado corretamente.")

    pdf.output(output_path)

def generate_and_upload_pdf(student_name, insights, avg_error, video_url, output_path_local, output_path_r2, s3_client, bucket_name, full_feedback=None):
    generate_pdf_report(student_name, insights, avg_error, video_url, output_path_local, full_feedback)

    with open(output_path_local, "rb") as f:
        pdf_bytes = f.read()

    s3_client.put_object(
        Bucket=bucket_name,
        Key=output_path_r2,
        Body=pdf_bytes,
        ContentType='application/pdf'
    )

    return output_path_r2  # Retorna apenas a chave; o frontend já monta a URL completa

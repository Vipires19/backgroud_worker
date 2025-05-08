from fpdf import FPDF

def generate_pdf_report(student_name, insights, avg_error, video_url, output_path, full_feedback=None):
    pdf = PDF()
    pdf.add_page()

    pdf.chapter_title(f"Aluno: {student_name}")
    pdf.chapter_body(f"Erro médio total: {avg_error:.2f}\u00b0\n\n")

    pdf.chapter_title("Principais Corre\u00e7\u00f5es:")
    for insight in insights[:5]:
        pdf.chapter_body(f"- {insight}")

    if full_feedback:
        pdf.chapter_title("Feedback Inteligente Personalizado:")
        pdf.chapter_body(full_feedback)

    pdf.chapter_title("Link do v\u00eddeo comparativo:")
    pdf.chapter_body(video_url)

    pdf.output(output_path)

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 14)
        self.cell(0, 10, 'Relat\u00f3rio de An\u00e1lise de Exerc\u00edcio', 0, 1, 'C')
        self.ln(10)

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 10, body)
        self.ln()

def generate_and_upload_pdf(student_name, insights, avg_error, video_url, output_path_local, output_path_r2, s3_client, bucket_name, full_feedback=None):
    generate_pdf_report(student_name, insights, avg_error, video_url, output_path_local, full_feedback)

    with open(output_path_local, "rb") as f:
        pdf_bytes = f.read()

    s3_client.put_object(Bucket=bucket_name, Key=output_path_r2, Body=pdf_bytes, ContentType='application/pdf')
    return f"https://{bucket_name}.r2.cloudflarestorage.com/{output_path_r2}"

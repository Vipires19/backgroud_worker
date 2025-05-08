# personal_comparator/utils/openai_feedback.py

import openai

def generate_feedback_via_openai(joint_errors, openai_api_key):
    openai.api_key = openai_api_key

    # Monta a descrição dos erros
    errors_description = "\n".join([f"{joint}: diferença de {error:.1f} graus" for joint, error in joint_errors.items()])

    prompt = f"""
                Você é um personal trainer especialista.  
                Com base nas diferenças de ângulos articulares abaixo, gere um feedback detalhado para melhorar a execução do movimento do aluno.

                Dados:
                {errors_description}

                Gere recomendações específicas para correções técnicas e posturais, de forma clara e profissional.
                """

    response = openai.chat.completions.create(
        model="gpt-4",  # Pode usar "gpt-3.5-turbo" para custo mais baixo
        messages=[
            {"role": "system", "content": "Você é um personal trainer especialista."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.5,
        max_tokens=500,
    )

    return response.choices[0].message.content

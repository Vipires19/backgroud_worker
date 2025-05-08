import numpy as np

# Articulações que vamos analisar (tripletos de índices do MediaPipe)
JOINTS = {
    "Ombro Direito": (12, 14, 16),
    "Ombro Esquerdo": (11, 13, 15),
    "Cotovelo Direito": (14, 12, 24),
    "Cotovelo Esquerdo": (13, 11, 23),
    "Quadril Direito": (24, 26, 28),
    "Quadril Esquerdo": (23, 25, 27),
}

def calculate_angle(a, b, c):
    """Calcula o ângulo entre três pontos"""
    a = np.array([a.x, a.y])
    b = np.array([b.x, b.y])
    c = np.array([c.x, c.y])

    ba = a - b
    bc = c - b

    cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
    angle = np.arccos(np.clip(cosine_angle, -1.0, 1.0))
    return np.degrees(angle)

def analyze_poses(ref_landmarks, exec_landmarks):
    """
    Compara landmarks entre referência e execução.
    Retorna:
    - insights em texto
    - erro médio total
    - erros médios por articulação (para usar com OpenAI)
    """
    if not ref_landmarks or not exec_landmarks:
        return [], 0.0, {}

    diffs = []
    for ref, exe in zip(ref_landmarks, exec_landmarks):
        for joint, (a, b, c) in JOINTS.items():
            angle_ref = calculate_angle(ref[a], ref[b], ref[c])
            angle_exe = calculate_angle(exe[a], exe[b], exe[c])
            diff = abs(angle_ref - angle_exe)
            diffs.append((joint, diff))

    # Agrupa os erros por articulação
    errors_by_joint = {}
    for joint, diff in diffs:
        if joint not in errors_by_joint:
            errors_by_joint[joint] = []
        errors_by_joint[joint].append(diff)

    # Calcula os erros médios por articulação
    avg_errors = {joint: np.mean(errors) for joint, errors in errors_by_joint.items()}
    avg_error_total = np.mean(list(avg_errors.values()))

    # Gera insights amigáveis
    insights = []
    for joint, avg_diff in sorted(avg_errors.items(), key=lambda x: x[1], reverse=True):
        if avg_diff > 10:  # Só mostra erros relevantes
            insights.append(f"Ajustar {joint}: diferença média de {avg_diff:.1f}°.")

    return insights, avg_error_total, avg_errors

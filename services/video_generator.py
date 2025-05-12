import cv2
import numpy as np
import mediapipe as mp
import os
import tempfile
from mediapipe.framework.formats import landmark_pb2

mp_drawing = mp.solutions.drawing_utils
mp_pose = mp.solutions.pose

def draw_landmarks_on_frame(frame, landmarks_list):
    if landmarks_list:
        mp_landmarks = landmark_pb2.NormalizedLandmarkList(landmark=landmarks_list)
        mp_drawing.draw_landmarks(
            frame,
            mp_landmarks,
            mp_pose.POSE_CONNECTIONS,
            landmark_drawing_spec=mp_drawing.DrawingSpec(color=(0, 255, 0), thickness=2, circle_radius=2),
            connection_drawing_spec=mp_drawing.DrawingSpec(color=(0, 0, 255), thickness=2, circle_radius=2)
        )
    return frame

from moviepy.editor import ImageSequenceClip

def generate_comparative_video(frames_ref, landmarks_ref, frames_exec, landmarks_exec):
    if len(frames_ref) == 0 or len(frames_exec) == 0:
        print("[ERRO] Lista de frames vazia.")
        return None

    target_width = 480
    target_height = 270
    min_frames = max(len(frames_ref), len(frames_exec))
    combined_frames = []

    for i in range(min_frames):
        try:
            frame_ref = frames_ref[i] if i < len(frames_ref) else frames_ref[-1]
            frame_exec = frames_exec[i] if i < len(frames_exec) else frames_exec[-1]
            landmark_ref = landmarks_ref[i] if i < len(landmarks_ref) else landmarks_ref[-1]
            landmark_exec = landmarks_exec[i] if i < len(landmarks_exec) else landmarks_exec[-1]

            frame_ref = draw_landmarks_on_frame(frame_ref.copy(), landmark_ref)
            frame_exec = draw_landmarks_on_frame(frame_exec.copy(), landmark_exec)

            frame_ref = cv2.resize(frame_ref, (target_width, target_height))
            frame_exec = cv2.resize(frame_exec, (target_width, target_height))

            combined = np.hstack((frame_ref, frame_exec))
            combined_rgb = cv2.cvtColor(combined, cv2.COLOR_BGR2RGB)
            combined_frames.append(combined_rgb)
        except Exception as e:
            print(f"[ERRO] Erro ao processar frame {i}: {e}")

    # Cria arquivo temporário seguro
    temp_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    temp_output_path = temp_file.name
    temp_file.close()

    try:
        print(f"[INFO] Gerando vídeo com MoviePy: {temp_output_path}")
        clip = ImageSequenceClip(combined_frames, fps=30)
        clip.write_videofile(temp_output_path, codec="libx264", audio=False, logger=None)
        print(f"[INFO] Vídeo gerado com sucesso: {temp_output_path}")
    except Exception as e:
        print(f"[ERRO] Falha ao gerar vídeo com MoviePy: {e}")
        return None

    if not os.path.exists(temp_output_path):
        print("[ERRO] Vídeo não foi gerado no caminho esperado.")
        return None

    try:
        with open(temp_output_path, "rb") as f:
            video_bytes = f.read()
        os.remove(temp_output_path)
        return video_bytes
    except Exception as e:
        print(f"[ERRO] Falha ao ler ou apagar o vídeo gerado: {e}")
        return None

def save_and_upload_comparative_video(frames_ref, landmarks_ref, frames_exec, landmarks_exec, upload_path, s3_client, bucket_name):
    print("[UPLOAD] Iniciando geração do vídeo comparativo...", flush=True)

    video_path = generate_comparative_video(frames_ref, landmarks_ref, frames_exec, landmarks_exec)

    if not video_path:
        print("[ERRO] Vídeo não foi gerado no caminho esperado.", flush=True)
        return None

    try:
        s3_client.upload_file(
            Filename=video_path,
            Bucket=bucket_name,
            Key=upload_path,
            ExtraArgs={"ContentType": "video/mp4"}
        )
        print(f"[UPLOAD] Vídeo enviado com sucesso para {upload_path}", flush=True)
        os.remove(video_path)
        return upload_path
    except Exception as e:
        print(f"[ERRO] Falha ao subir vídeo para R2: {e}", flush=True)
        return None

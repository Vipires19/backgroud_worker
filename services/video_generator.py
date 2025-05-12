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

def generate_comparative_video(frames_ref, landmarks_ref, frames_exec, landmarks_exec):
    if len(frames_ref) == 0 or len(frames_exec) == 0:
        return None

    target_width = 480
    target_height = 270
    min_frames = max(len(frames_ref), len(frames_exec))

    # Cria arquivo temporário seguro
    temp_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    temp_output_path = temp_file.name
    temp_file.close()

    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(temp_output_path, fourcc, 30.0, (target_width * 2, target_height))

    for i in range(min_frames):
        frame_ref = frames_ref[i] if i < len(frames_ref) else frames_ref[-1]
        frame_exec = frames_exec[i] if i < len(frames_exec) else frames_exec[-1]
        landmark_ref = landmarks_ref[i] if i < len(landmarks_ref) else landmarks_ref[-1]
        landmark_exec = landmarks_exec[i] if i < len(landmarks_exec) else landmarks_exec[-1]

        frame_ref = draw_landmarks_on_frame(frame_ref.copy(), landmark_ref)
        frame_exec = draw_landmarks_on_frame(frame_exec.copy(), landmark_exec)

        frame_ref = cv2.resize(frame_ref, (target_width, target_height))
        frame_exec = cv2.resize(frame_exec, (target_width, target_height))

        combined = np.hstack((frame_ref, frame_exec))
        out.write(combined)

    out.release()

    with open(temp_output_path, "rb") as f:
        video_bytes = f.read()

    os.remove(temp_output_path)
    return video_bytes

def save_and_upload_comparative_video(frames_ref, landmarks_ref, frames_exec, landmarks_exec, upload_path, s3_client, bucket_name):
    print("[UPLOAD] Iniciando geração do vídeo comparativo...")

    video_bytes = generate_comparative_video(frames_ref, landmarks_ref, frames_exec, landmarks_exec)
    
    if not video_bytes:
        print("[ERRO] Vídeo não foi gerado. Bytes estão vazios.")
        return None

    print(f"[UPLOAD] Tamanho do vídeo gerado: {len(video_bytes)} bytes")

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=upload_path,
            Body=video_bytes,
            ContentType='video/mp4',
            #ACL='public-read'  # <-- só se seu bucket permitir isso
        )
        print(f"[UPLOAD] Vídeo enviado com sucesso para {upload_path}")
        return f"{upload_path}"
    except Exception as e:
        print(f"[ERRO] Falha ao subir vídeo para R2: {e}")
        return None

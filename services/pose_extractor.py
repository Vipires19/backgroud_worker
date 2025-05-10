import cv2
import mediapipe as mp

mp_pose = mp.solutions.pose

def extract_landmarks_from_video(video_path, max_frames=300):
    """Extrai landmarks e frames de um vídeo usando MediaPipe (limitado a max_frames)."""
    cap = cv2.VideoCapture(video_path)
    frames = []
    landmarks_list = []

    if not cap.isOpened():
        print(f"[ERRO] Não foi possível abrir o vídeo: {video_path}")
        return [], []

    with mp_pose.Pose(static_image_mode=False, model_complexity=1, enable_segmentation=False) as pose:
        frame_count = 0

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret or frame_count >= max_frames:
                break

            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = pose.process(frame_rgb)

            if results.pose_landmarks:
                frames.append(frame.copy())  # Guarda o frame original
                landmarks_list.append(results.pose_landmarks.landmark)

            frame_count += 1

    cap.release()
    return frames, landmarks_list

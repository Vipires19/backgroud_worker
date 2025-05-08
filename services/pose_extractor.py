# personal_comparator/services/pose_extractor.py

import cv2
import mediapipe as mp

mp_pose = mp.solutions.pose

def extract_landmarks_from_video(video_path):
    """Extrai os landmarks e frames de um vídeo usando MediaPipe."""
    cap = cv2.VideoCapture(video_path)
    frames = []
    landmarks_list = []

    with mp_pose.Pose(static_image_mode=False, model_complexity=1, enable_segmentation=False) as pose:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            results = pose.process(frame_rgb)

            if results.pose_landmarks:
                frames.append(frame.copy())  # Guarda o frame original
                landmarks_list.append(results.pose_landmarks.landmark)

    cap.release()
    return frames, landmarks_list

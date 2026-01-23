import cv2
import json
import base64
import numpy as np
import requests
from roboflow import Roboflow
from flask import Blueprint, jsonify

# --- CONFIGURACIÓN (Idealmente esto iría en variables de entorno .env) ---
ROBOFLOW_API_KEY = "Meni7XPRgKOEkHJeXRHz"
PLANT_ID_API_KEY = "ykRbohCpTwwOb701JFAVZAWngD1LgF4JQNxT74l6rCEZQof5qP"
PLANT_ID_ENDPOINT = "https://api.plant.id/v3/identification"

class AgroEngine:
    def __init__(self):
        # Inicializamos Roboflow una sola vez al arrancar la clase
        self.rf = Roboflow(api_key=ROBOFLOW_API_KEY)
        self.project = self.rf.workspace("agridrone-pblcc").project("agridetect")
        self.version = self.project.version(3)
        self.model = self.version.model

    def _process_roboflow(self, image_bytes):
        """Procesa la imagen para detectar Cielo y Suelo usando YOLO."""
        try:
            # Convertir bytes a numpy array para OpenCV
            nparr = np.frombuffer(image_bytes, np.uint8)
            image_numpy = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if image_numpy is None:
                return {"error": "No se pudo decodificar la imagen para CV2"}

            # Predicción (con confianza base del 40%)
            response = self.model.predict(image_numpy, confidence=40).json()

            # Normalización de respuesta
            detections_list = []
            if isinstance(response, dict) and 'predictions' in response:
                detections_list = response['predictions']
            elif isinstance(response, list):
                detections_list = response

            # Lógica de selección (Target Mapping)
            target_mapping = {
                "sky": ["sky", "cielo", "cloud"], 
                "soil": ["crop", "soil", "field", "ground", "agriculture-land", "land"]
            }

            final_result = {"sky": None, "soil": None}

            for key, possible_labels in target_mapping.items():
                best_candidate = None
                max_confidence = 0
                for det in detections_list:
                    if det['class'].lower() in possible_labels:
                        if det['confidence'] > max_confidence:
                            max_confidence = det['confidence']
                            best_candidate = det
                
                if best_candidate:
                    final_result[key] = {
                        "x": int(best_candidate['x']),
                        "y": int(best_candidate['y']),
                        "confidence": float(best_candidate['confidence'])
                    }
            return final_result

        except Exception as e:
            return {"error": f"Fallo en Roboflow: {str(e)}"}

    def _process_plant_id(self, image_bytes):
        """Procesa la imagen para identificar cultivo y enfermedades."""
        try:
            # Codificar bytes directamente a Base64
            base64_img = base64.b64encode(image_bytes).decode("utf-8")

            headers = {
                "Content-Type": "application/json",
                "Api-Key": PLANT_ID_API_KEY
            }
            
            data = {
                "images": [base64_img],
                "latitude": 40.4168, # Podríamos parametrizar esto en el futuro
                "longitude": -3.7038,
                "health": "all",
                "similar_images": True
            }

            response = requests.post(PLANT_ID_ENDPOINT, headers=headers, json=data)
            response.raise_for_status()
            api_response = response.json()

            # --- Parser Backend (Tu lógica original adaptada) ---
            if not api_response or 'result' not in api_response:
                return {"error": "Respuesta vacía de Plant.id"}

            result = api_response['result']
            parsed_data = {}

            # 1. Identificación
            classification = result.get('classification', {}).get('suggestions', [])
            if classification:
                top_match = classification[0]
                parsed_data['plant_name'] = top_match['name']
                parsed_data['plant_confidence'] = top_match['probability']
                details = top_match.get('details', {})
                parsed_data['common_name'] = details.get('common_names', ["Desconocido"])[0]
                parsed_data['info_url'] = details.get('url', None)

            # 2. Salud
            is_healthy = result.get('is_healthy', {})
            parsed_data['healthy_prob'] = is_healthy.get('probability', 0)

            # 3. Enfermedades
            disease_suggestions = result.get('disease', {}).get('suggestions', [])
            parsed_data['diseases'] = []
            if disease_suggestions:
                for d in disease_suggestions:
                    if d['probability'] > 0.10: 
                        parsed_data['diseases'].append({
                            'name': d['name'],
                            'probability': d['probability'],
                            'description': d.get('details', {}).get('description', 'Sin descripción.')[:200]
                        })

            return parsed_data

        except Exception as e:
            return {"error": f"Fallo en Plant.id: {str(e)}"}

    def analyze_full(self, image_bytes):
        """Método maestro que llama a ambos motores."""
        # Nota: En el paso 2 haremos que esto sea asíncrono para velocidad
        roi_data = self._process_roboflow(image_bytes)
        plant_data = self._process_plant_id(image_bytes)

        return {
            "telemetry_roi": roi_data,
            "biological_data": plant_data
        }
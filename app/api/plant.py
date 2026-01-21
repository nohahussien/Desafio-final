from flask import Blueprint, request, jsonify
from agro_engine import AgroEngine
import traceback

# 1. Definimos el Blueprint
# El nombre 'plant_api' es interno para Flask.
plant_bp = Blueprint('plant_api', __name__)

# 2. Inicializamos el motor
# Se hace aquí para que al importar este archivo, el motor arranque.
try:
    print("🌿 [plant.py] Inicializando Motor AgroEngine...")
    engine = AgroEngine()
    print("✅ [plant.py] Motor listo.")
except Exception as e:
    print(f"❌ [plant.py] Error cargando el motor: {e}")
    engine = None

# --- RUTAS ---

@plant_bp.route('/analyze', methods=['POST'])
def analyze_plant_data():
    """
    Endpoint principal. Recibe una imagen y devuelve análisis unificado.
    Ruta esperada: POST /api/v1/plant/analyze (depende del prefix en el main)
    """
    # Verificaciones de seguridad básicas
    if not engine:
        return jsonify({"error": "El motor de IA no está disponible en este momento"}), 503

    if 'image' not in request.files:
        return jsonify({"error": "No se recibió el archivo 'image'"}), 400
    
    file = request.files['image']
    
    if file.filename == '':
        return jsonify({"error": "Nombre de archivo vacío"}), 400

    try:
        # Leemos los bytes
        image_bytes = file.read()

        # Llamamos al motor (AgroEngine)
        results = engine.analyze_full(image_bytes)

        # Devolvemos el JSON estándar
        return jsonify({
            "status": "success",
            "source": "plant_module",
            "data": results
        }), 200

    except Exception as e:
        print(f"❌ Error en endpoint analyze: {e}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@plant_bp.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar que este módulo específico está vivo"""
    return jsonify({
        "module": "plant.py", 
        "status": "active", 
        "engine_loaded": engine is not None
    }), 200
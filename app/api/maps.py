import requests
import os
from flask import Blueprint, request, jsonify
from .sentinel_service import SentinelService
from models.field import getParcelas4HistMeteo
import json

# Creamos el Blueprint
sentinel_bp = Blueprint('sentinel_bp', __name__, url_prefix='/agrosync-api')

@sentinel_bp.route('/maps_sentinel', methods=['POST'])
def analyze_field():
    """
    Endpoint principal.
    Espera un JSON con la estructura:
    {
        "uid_parcel": "Id_Finca",
    }
    """
    try:
        # 1. Recibir los datos (El "Body" de la petición)
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No se recibieron datos JSON"}), 400
            
        finca_id = data.get('uid_parcel')
        #wkt_polygon = data.get('wkt')
        parcelas = getParcelas4HistMeteo(finca_id)
        
        for row in parcelas:
            uid_parcel = row["uid_parcel"]
            coords = row["coordinates_parcel"]
            strCoordsPolygon = convertirArrayCoordenadasEnPoligono(coords)
            # 3. Invocar al Servicio (El experto)
            service = SentinelService()
            result = service.analyze_polygon(strCoordsPolygon)

            if not result:
                return jsonify({"error": "No se pudieron obtener imágenes recientes o válidas"}), 404

            # 4. Añadir el ID al resultado final para mantener la trazabilidad
            result["uid_parcel"] = uid_parcel
            
            return jsonify(result), 200
        return jsonify({"error": f"No se ha introducido uid_parcel"}), 400
    except Exception as e:
        # En producción, aquí deberíamos hacer logging del error real
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500
    


def convertirArrayCoordenadasEnPoligono(coords):
    """
    Convierte [[lat1, lon1], [lat2, lon2], ...] a 'POLYGON((lon1 lat1, lon2 lat2, ...))'
    
    Args:
        coords: list de [lat, lon] como lo recibe de fullstack
    
    Returns:
        str en formato WKT para Auravant API
    """
    if isinstance(coords, str):
        coords = json.loads(coords)
    if not coords or len(coords) < 3:
        raise ValueError("El polígono necesita al menos 3 coordenadas")
    # Extrae lon lat (invierte orden) y formatea
    pairs = [f"{lon} {lat}" for lat, lon in coords]
    # Asegura cierre (primer punto == último)
    if pairs[0] != pairs[-1]:
        pairs.append(pairs[0])
    
    return f"POLYGON(({', '.join(pairs)}))"
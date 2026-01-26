from flask import Blueprint, jsonify, request
from models.convers import get_conversations, create_new_conversation, get_messages_by_conversation

conversations_bp = Blueprint('conversations', __name__, url_prefix='/agrosync-api/chat')

@conversations_bp.route('/conversations', methods=['GET'])
def api_get_conversations():
    """Endpoint GET /api/conversations?firebase_uid_user=xxx"""
    firebase_uid_user = request.args.get('firebase_uid_user')
    conversations = get_conversations(firebase_uid_user)
    return jsonify(conversations)


@conversations_bp.route('/new_conversation', methods=['POST'])
def api_new_conversation():
    """POST /chat/new_conversation"""
    try:
        data = request.get_json()
        firebase_uid_user = data.get('firebase_uid_user')
        titulo = data.get('titulo', 'Nueva conversación')
        
        if not firebase_uid_user:
            return jsonify({"error": "firebase_uid_user requerido"}), 400
        
        conversation = create_new_conversation(firebase_uid_user, titulo)
        
        if not conversation:
            return jsonify({"error": "Error al crear conversación"}), 500
        
        # Formato exacto para frontend
        return jsonify({
            "id": conversation['id'],
            "title": conversation['titulo'],
            "timestamp": conversation['created_at'].isoformat()
        }), 201
        
    except Exception as e:
        return jsonify({"error": "Error interno", "details": str(e)}), 500
    
@conversations_bp.route('/conversations/<conversation_id>/messages', methods=['GET'])
def api_get_messages(conversation_id):
    """GET /chat/conversations/{id}/messages"""
    messages = get_messages_by_conversation(conversation_id)
    return jsonify(messages)

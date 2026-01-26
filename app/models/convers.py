from app.core.database import get_db_connection
from psycopg2.extras import RealDictCursor
from flask import request


def get_conversations(firebase_uid_user=None):
    """
    Obtiene conversaciones del usuario
    Retorna lista de dicts con formato para frontend
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                c.id::text as id,
                c.titulo as title,
                c.created_at as timestamp,
                c.firebase_uid_user,
                c.descripcion
            FROM conversacion c
        """
        params = []
        
        if firebase_uid_user is not None:
            query += " WHERE c.firebase_uid_user = %s"
            params.append(firebase_uid_user)
        
        query += " ORDER BY c.created_at DESC"
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            conversations = cur.fetchall()
            return conversations  # Lista de dicts lista para JSON

    except Exception as e:
        conn.rollback()
        print(f"Error en get_conversations: {e}")
        return []
    finally:
        conn.close()




def create_new_conversation(firebase_uid_user, titulo="Nueva conversación"):
    """
    Crea una nueva conversación y retorna sus datos
    """
    conn = get_db_connection()
    try:
        query = """
            INSERT INTO conversacion (firebase_uid_user, titulo, descripcion)
            VALUES (%s, %s, '')
            RETURNING id, titulo, created_at
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (firebase_uid_user, titulo))
            new_conversation = cur.fetchone()
            conn.commit()
            return new_conversation  # Dict con id, titulo, created_at

    except Exception as e:
        conn.rollback()
        print(f"Error en create_new_conversation: {e}")
        return None
    finally:
        conn.close()


def get_messages_by_conversation(conversation_id):
    """
    Obtiene todos los mensajes de una conversación específica
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                id::text as id,
                rol as role,
                contenido as content,
                created_at as timestamp
            FROM mensaje 
            WHERE conversacion_id = %s
            ORDER BY created_at ASC
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (conversation_id,))
            messages = cur.fetchall()
            return messages

    except Exception as e:
        conn.rollback()
        print(f"Error en get_messages_by_conversation: {e}")
        return []
    finally:
        conn.close()

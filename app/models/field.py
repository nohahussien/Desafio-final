from sqlalchemy import Column, Integer, String  # ← Quita Base
# from app.core.database import Base  # ← ELIMINA ESTA LÍNEA
from app.core.database import get_db_connection
import psycopg2
from psycopg2.extras import RealDictCursor

def parcelaExiste(uid_producer: str, coords: str):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Verificar si email ya existe
            cur.execute("SELECT uid_parcel FROM public.parcels where uid_producer = %s and coordinates_parcel like  %s", (uid_producer, coords))
            if cur.fetchone():
                return "Error||||La parcerla existe para el productor de la sesión"   
            else:
                return "OK|||ParcelaLibre"
            
    except Exception as e:
        conn.rollback()
        print(f"Error en register_user: {e}")
        return None
    finally:
        conn.close()
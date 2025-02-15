import requests
import pandas as pd
import psycopg2

def extraer_datos():
    """
    Extrae datos de la API de tasas de cambio.
    """
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    respuesta = requests.get(url)
    
    if respuesta.status_code == 200:
        return respuesta.json()
    else:
        raise Exception(f"Error en la solicitud: {respuesta.status_code}")

def transformar_datos(datos):
    """
    Transforma el diccionario de tasas en un DataFrame.
    """
    tasas = datos.get("rates", {})
    df = pd.DataFrame(list(tasas.items()), columns=["currency", "rate"])
    df = df.sort_values(by="rate", ascending=False)
    return df

def conectar_db():
    """
    Establece la conexión con la base de datos PostgreSQL.
    Asegurate de reemplazar 'tu_password' por la contraseña correcta.
    """
    try:
        conexion = psycopg2.connect(
            host="localhost",        # Dirección del servidor
            database="etl_db", 
            port=5434,      # Nombre de la base de datos creada
            user="postgres",         # Usuario (por defecto 'postgres')
            password="tucuman"   # Reemplazar con la contraseña real
        )
        return conexion
    except Exception as e:
        print("Error al conectar a la base de datos:", e)
        raise

def cargar_datos(df):
    """
    Crea la tabla exchange_rates (si no existe) e inserta los datos.
    """
    conexion = conectar_db()
    cursor = conexion.cursor()
    try:
        # Crear la tabla si no existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id SERIAL PRIMARY KEY,
                currency VARCHAR(10),
                rate NUMERIC
            );
        """)
        conexion.commit()
        
        # Insertar cada fila del DataFrame
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO exchange_rates (currency, rate) VALUES (%s, %s)",
                (row["currency"], row["rate"])
            )
        conexion.commit()
        print("Datos cargados en PostgreSQL.")
    except Exception as e:
        conexion.rollback()
        print("Error al cargar datos:", e)
        raise
    finally:
        cursor.close()
        conexion.close()

def main():
    try:
        print("Extrayendo datos...")
        datos = extraer_datos()
        print("Datos extraídos:")
        print(datos)
        
        print("\nTransformando datos...")
        df_tasas = transformar_datos(datos)
        print("Datos transformados:")
        print(df_tasas.head())
        
        print("\nCargando datos en la base de datos...")
        cargar_datos(df_tasas)
        
        print("\nProceso ETL completado exitosamente.")
    except Exception as e:
        print("Ocurrió un error:", e)

if __name__ == "__main__":
    main()

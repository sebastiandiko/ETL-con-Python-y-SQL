import requests

def extraer_datos():
    # URL de la API (en este ejemplo usamos la API de tasas de cambio)
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    respuesta = requests.get(url)
    
    if respuesta.status_code == 200:
        datos = respuesta.json()
        return datos
    else:
        raise Exception(f"Error en la solicitud: {respuesta.status_code}")

if __name__ == "__main__":
    datos = extraer_datos()
    print("Datos extraídos:", datos)

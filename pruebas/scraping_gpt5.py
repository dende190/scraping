# tuboleta_scraper.py
import os
import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import csv

# ------------- Configuración -------------
URL = (
    "https://tuboleta.com/es/resultados-de-busqueda"
    "?ciudades=12012&categorias=26838&fecha_inicio=&fecha_final=&s="
)
OUTPUT_FILE = "data/tuboleta_eventos.csv"

# ------------- Función para obtener HTML renderizado -------------
def obtener_html_renderizado(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print(f"Abriendo URL: {url}")
        page.goto(url, timeout=60000)
        page.wait_for_load_state("networkidle")
        contenido = page.content()
        browser.close()
        return contenido

# ------------- Función para parsear eventos desde HTML -------------
def parsear_eventos(html: str):
    soup = BeautifulSoup(html, "html.parser")
    eventos = []
    # Identifica el contenedor de cada evento (adapta selectores si cambian)
    print(soup.select("div.es-card"));
    for tarjeta in soup.select("div.es-card"):  # Ajusta si el sitio cambia
        titulo_tag = tarjeta.select_one("h3.es-title")
        fecha_tag = tarjeta.select_one("p.es-date")
        lugar_tag = tarjeta.select_one("p.es-location")
        link_tag = tarjeta.select_one("a.es-card-link")

        if not titulo_tag or not link_tag:
            continue

        titulo = titulo_tag.get_text(strip=True)
        fecha = fecha_tag.get_text(strip=True) if fecha_tag else ""
        lugar = lugar_tag.get_text(strip=True) if lugar_tag else ""
        url_evento = link_tag["href"]
        # Asegúrate de que sea URL absoluta
        if url_evento.startswith("/"):
            url_evento = "https://tuboleta.com" + url_evento

        eventos.append({
            "titulo": titulo,
            "fecha": fecha,
            "lugar": lugar,
            "url": url_evento
        })
    return eventos

# ------------- Función para guardar en CSV -------------
def guardar_csv(eventos, ruta_salida: str):
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    campos = ["titulo", "fecha", "lugar", "url"]
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    archivo_temp = f"{ruta_salida}.tmp"
    with open(archivo_temp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for e in eventos:
            writer.writerow(e)
    os.replace(archivo_temp, ruta_salida)
    print(f"Guardado {len(eventos)} eventos en {ruta_salida} ({timestamp})")

# ------------- Función principal -------------
def main():
    html = obtener_html_renderizado(URL)
    eventos = parsear_eventos(html)
    if eventos:
        guardar_csv(eventos, OUTPUT_FILE)
    else:
        print("No se encontraron eventos. Revisa los selectores o el contenido HTML.")

if __name__ == "__main__":
    main()

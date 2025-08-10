from bs4 import BeautifulSoup
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Browser, Page
from psycopg2.extras import RealDictCursor
from spotify_api import buscarArtistaDatosPorIdVinculante
import asyncio
import httpx
import os
import psycopg2
import random
import time

TUBOLETA_ENLACE = 'https://tuboleta.com'
TUBOLETA_CONCIERTOS_ENLACE = (
  TUBOLETA_ENLACE +
  '/es/resultados-de-busqueda' +
  '?ciudades=12012' +
  '&categorias=26838' +
  '&page='
);
PAGINA_TIEMPO_ESPERA_EN_MILISEGUNDOS = 60000
SELECTORES_POR_NOMBRE = {
  'paginaVaciaMensaje': '.view-empty',
  'conciertoTarjetaEnlace': '.content-link-container',
  'conciertoTarjetaInformacion': '.content-info div',
  'conciertoTarjetaFecha': '.dates-container span',
}
TUBOLETA_PAGINACION_MAXIMA = 30
ENCABEZADO_USUARIO_AGENTE = (
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
)

playwright = None
def ejecutarPostgresGuardarConsulta(postgresConsulta: str):
  postgresConexion = (
    psycopg2
    .connect(
      dbname=os.getenv('POSTGRES_BASE_DATOS'),
      user=os.getenv('POSTGRES_USUARIO'),
      password=os.getenv('POSTGRES_CONTRASENA'),
      host=os.getenv('POSTGRES_ENLACE'),
      port='5432',
    )
  )

  postgres = postgresConexion.cursor()
  postgres.execute(postgresConsulta)
  postgresConexion.commit()
  postgres.close()
  postgresConexion.close()

def ejecutarPostgresConsulta(postgresConsulta: str):
  postgresConexion = (
    psycopg2
    .connect(
      dbname=os.getenv('POSTGRES_BASE_DATOS'),
      user=os.getenv('POSTGRES_USUARIO'),
      password=os.getenv('POSTGRES_CONTRASENA'),
      host=os.getenv('POSTGRES_ENLACE'),
      port='5432',
    )
  )

  postgres = postgresConexion.cursor(cursor_factory=RealDictCursor)
  postgres.execute(postgresConsulta)
  resultados = postgres.fetchall()
  postgres.close()
  postgresConexion.close()

  return resultados

async def abrirNavegador() -> Browser:
  global playwright
  playwright = await async_playwright().start()
  navegador = await playwright.chromium.launch(headless=True)
  print('Navegador abierto')
  return navegador

async def cerrarNavegador(navegador: Browser):
  await navegador.close();
  await playwright.stop()
  print('Navegador cerrado')

async def obtenerHtmlParseado(navegador: Browser, enlace: str) -> BeautifulSoup:
  navegadorContexto = await navegador.new_context(user_agent=ENCABEZADO_USUARIO_AGENTE)
  navegadorPestana = await navegadorContexto.new_page()
  await navegadorPestana.goto(enlace, timeout=PAGINA_TIEMPO_ESPERA_EN_MILISEGUNDOS)
  await navegadorPestana.wait_for_load_state('networkidle')
  navegadorPestanaContenido = await navegadorPestana.content()
  htmlParseado = BeautifulSoup(navegadorPestanaContenido, 'html.parser')
  print(f'Enlace abierto: {enlace}')
  return htmlParseado

def removerComillas(texto: str) -> str:
  return texto.replace('"', '').replace('\'', '')

async def obtenerConciertosPorIdentificadorSemantico(navegador: Browser) -> dir:
  conciertosPorIdentificadorSemantico = {}
  tuboletaConciertosPaginaActual = 1
  while (
    True and
    (tuboletaConciertosPaginaActual < TUBOLETA_PAGINACION_MAXIMA)
  ):
    tuboletaConciertosEnlace = (
      TUBOLETA_CONCIERTOS_ENLACE +
      str(tuboletaConciertosPaginaActual)
    )
    conciertosHtmlParseado = await (
      obtenerHtmlParseado(
        navegador=navegador,
        enlace=tuboletaConciertosEnlace,
      )
    )

    if (
      conciertosHtmlParseado
      .select_one(SELECTORES_POR_NOMBRE['paginaVaciaMensaje'])
    ):
      print('Se terminaron los conciertos')
      break

    dConciertosTarjetasEnlace = (
      conciertosHtmlParseado
      .select(SELECTORES_POR_NOMBRE['conciertoTarjetaEnlace'])
    )
    for dConciertoTarjetaEnlace in dConciertosTarjetasEnlace:
      conciertoEnlace = dConciertoTarjetaEnlace['href']
      dConciertoTarjetaInformaciones = (
        dConciertoTarjetaEnlace
        .select(SELECTORES_POR_NOMBRE['conciertoTarjetaInformacion'])
      )
      conciertoNombre = dConciertoTarjetaInformaciones[0].get_text().strip()
      conciertoLugar = dConciertoTarjetaInformaciones[1].get_text().strip()
      conciertoFecha = (
        dConciertoTarjetaEnlace
        .select(SELECTORES_POR_NOMBRE['conciertoTarjetaFecha'])
        [-1]
        .get_text()
        .strip()
      )
      conciertoIdentificadorSemantico = (
        conciertoEnlace.replace('/es/eventos/', '')
      )
      conciertosPorIdentificadorSemantico[conciertoIdentificadorSemantico] = {
        'nombre': removerComillas(conciertoNombre),
        'fecha': removerComillas(conciertoFecha),
        'lugar': removerComillas(conciertoLugar),
        'enlace': (TUBOLETA_ENLACE + conciertoEnlace),
      }

    tuboletaConciertosPaginaActual += 1
    time.sleep(random.uniform(1, 6))

  return conciertosPorIdentificadorSemantico

def obtenerConciertosNuevos(conciertosPorIdentificadorSemantico: dir) -> dir:
  postgresObtenerConciertosConsulta = (
    'SELECT identificador_semantico FROM concierto'
  )
  conciertos = ejecutarPostgresConsulta(postgresObtenerConciertosConsulta);
  if not conciertos:
    return conciertosPorIdentificadorSemantico

  conciertosExistentesPorIdentificadorSemantico = {}
  for concierto in conciertos:
    (
      conciertosExistentesPorIdentificadorSemantico
      [concierto['identificador_semantico']]
    ) = True

  identificadoresSemanticosExistentes = []
  for identificadorSemantico in conciertosPorIdentificadorSemantico.keys():
    if (
      identificadorSemantico not in
      conciertosExistentesPorIdentificadorSemantico
    ):
      continue

    identificadoresSemanticosExistentes.append(identificadorSemantico)

  for identificadorSemanticoExistente in identificadoresSemanticosExistentes:
    del conciertosPorIdentificadorSemantico[identificadorSemanticoExistente]

  return conciertosPorIdentificadorSemantico

def guardarConciertos(conciertosPorIdentificadorSemantico: dir) -> dir:
  conciertosAInsertarValores = []
  for identificadorSemantico, concierto in conciertosPorIdentificadorSemantico.items():
    conciertoValores = (
      '(' +
        f'\'{identificadorSemantico}\', ' +
        f'\'{concierto['nombre']}\', ' +
        f'\'{concierto['fecha']}\', ' +
        f'\'{concierto['lugar']}\', ' +
        f'\'{concierto['enlace']}\'' +
      ')'
    )
    conciertosAInsertarValores.append(conciertoValores)

  conciertosAInsertarConsulta = (
    'INSERT INTO concierto(' +
      'identificador_semantico, ' +
      'nombre, ' +
      'fecha, ' +
      'lugar, ' +
      'enlace' +
    ') VALUES ' +
    (','.join(conciertosAInsertarValores))
  )

  ejecutarPostgresGuardarConsulta(conciertosAInsertarConsulta)
  print(f'Se han guardado {len(conciertosAInsertarValores)} conciertos nuevos')

def agregarConciertosIds(
  conciertosPorIdentificadorSemantico: dir,
) -> dir:
  conciertosIdentificadoresSemanticos = []
  for identificadorSemantico in conciertosPorIdentificadorSemantico.keys():
    conciertosIdentificadoresSemanticos.append(identificadorSemantico)

  postgresObtenerConciertosCreadosConsulta = (
    'SELECT id, identificador_semantico ' +
    'FROM concierto ' +
    'WHERE ' +
      'identificador_semantico IN (\'' +
        f'{'\',\''.join(conciertosIdentificadoresSemanticos)}' +
      '\')'
  )
  conciertosCreados = (
    ejecutarPostgresConsulta(postgresObtenerConciertosCreadosConsulta)
  )

  for conciertoCreado in conciertosCreados:
    (
      conciertosPorIdentificadorSemantico
      [conciertoCreado['identificador_semantico']]
      ['id']
    ) = conciertoCreado['id']

  return conciertosPorIdentificadorSemantico

async def obtenerArtistas(conciertosPorIdentificadorSemantico: dir) -> dir:
  async with httpx.AsyncClient() as httpxCliente:
    tareasCola = [
      buscarArtistaDatosPorIdVinculante(
        httpxCliente=httpxCliente,
        nombre=concierto['nombre'],
        idVinculante=concierto['id'],
      )
      for concierto in conciertosPorIdentificadorSemantico.values()
    ]
    artistasPorConciertoId = await asyncio.gather(*tareasCola)

    return artistasPorConciertoId

def guardarArtistas(artistasPorConciertoId: dir):
  artistasAInsertarValores = []
  for artistaPorConciertoId in artistasPorConciertoId:
    for conciertoId, artista in artistaPorConciertoId.items():
      artistaValores = (
        '(' +
          f'\'{conciertoId}\', ' +
          f'\'{artista['nombre']}\', ' +
          f'\'{artista['seguidores']}\', ' +
          f'\'{artista['popularidad']}\', ' +
          f'\'{artista['enlace']}\'' +
        ')'
      )
      artistasAInsertarValores.append(artistaValores)

  artistasAInsertarConsulta = (
    'INSERT INTO artista_concierto(' +
      'concierto_id, ' +
      'nombre, ' +
      'seguidores, ' +
      'popularidad, ' +
      'enlace' +
    ') VALUES ' +
    (','.join(artistasAInsertarValores))
  )

  ejecutarPostgresGuardarConsulta(artistasAInsertarConsulta)
  print(f'Se han guardado {len(artistasAInsertarValores)} artistas nuevos')

async def inicializarBusqueda():
  load_dotenv()
  navegador = None;
  try:
    navegador = await abrirNavegador()
    conciertosPorIdentificadorSemantico = await (
      obtenerConciertosPorIdentificadorSemantico(navegador=navegador)
    )
    conciertosNuevosPorIdentificadorSemantico = (
      obtenerConciertosNuevos(conciertosPorIdentificadorSemantico)
    )
    if conciertosNuevosPorIdentificadorSemantico:
      guardarConciertos(conciertosNuevosPorIdentificadorSemantico)
      conciertosNuevosPorIdentificadorSemantico = (
        agregarConciertosIds(conciertosNuevosPorIdentificadorSemantico)
      )
      artistasPorConciertoId = await (
        obtenerArtistas(conciertosNuevosPorIdentificadorSemantico)
      )
      guardarArtistas(artistasPorConciertoId)
    else:
      print('No hay conciertos nuevos')

  finally:
    if navegador:
      await cerrarNavegador(navegador=navegador)

async def main():
  await inicializarBusqueda()

if __name__ == '__main__':
  asyncio.run(main())

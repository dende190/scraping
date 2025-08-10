from dotenv import load_dotenv
import asyncio
import base64
import httpx
import os
import time

SPOTIFY_ACCESO_LLAVE_ENLACE = 'https://accounts.spotify.com/api/token'
SPOTIFY_BUSQUEDA_API_ENLACE = 'https://api.spotify.com/v1/search'
accesoLlave = None
accesoLlaveTiempoUnixExpiracion = 0

async def obtenerSpotifyLlave(httpxCliente: httpx.AsyncClient) -> str:
  global accesoLlave, accesoLlaveTiempoUnixExpiracion
  if accesoLlave and time.time() < accesoLlaveTiempoUnixExpiracion:
    return accesoLlave

  load_dotenv()
  autenticacionCliente = (
    f'{os.getenv('SPOTIFY_CLIENTE_ID')}:' +
    f'{os.getenv('SPOTIFY_CLIENTE_SECRETO_ID')}'
  )
  autenticacionClienteBase64 = (
    base64.b64encode(autenticacionCliente.encode()).decode()
  )
  peticionEncabezados = {'Authorization': f'Basic {autenticacionClienteBase64}'}
  peticionDatos = {'grant_type': 'client_credentials'}

  peticionRespuesta = await (
    httpxCliente
    .post(
      SPOTIFY_ACCESO_LLAVE_ENLACE,
      headers=peticionEncabezados,
      data=peticionDatos
    )
  )
  peticionRespuesta.raise_for_status()
  accesoLlaveDatos = peticionRespuesta.json()

  accesoLlave = accesoLlaveDatos['access_token']
  accesoLlaveTiempoUnixExpiracion = (
    time.time() +
    accesoLlaveDatos['expires_in'] -
    30
  )

  return accesoLlave

async def buscarArtistaDatosPorIdVinculante(
  httpxCliente: httpx.AsyncClient,
  nombre: str,
  idVinculante: str,
) -> dir:
  accesoLlave = await obtenerSpotifyLlave(httpxCliente)

  peticionEncabezados = {'Authorization': f'Bearer {accesoLlave}'}
  peticionParametros = {'q': nombre, 'type': 'artist', 'limit': 1}

  async with httpx.AsyncClient() as client:
    while True:
      peticionRespuesta = await (
        httpxCliente
        .get(
          SPOTIFY_BUSQUEDA_API_ENLACE,
          headers=peticionEncabezados,
          params=peticionParametros
        )
      )

      if peticionRespuesta.status_code == 429:
        peticionReintentoTiempo = (
          int(peticionRespuesta.headers.get('Retry-After', 1))
        )
        print(
          f'Rate limit alcanzado, esperando {peticionReintentoTiempo} segundos...'
        )
        await asyncio.sleep(peticionReintentoTiempo)
        continue

      peticionRespuesta.raise_for_status()
      artistaBusqueda = peticionRespuesta.json()
      artistaBusquedaElementos = artistaBusqueda['artists']['items']
      if not artistaBusquedaElementos:
        return {}

      artista = artistaBusquedaElementos[0]
      return {
        idVinculante: {
          'id': artista['id'],
          'nombre': artista['name'],
          'seguidores': artista['followers']['total'],
          'popularidad': artista['popularity'],
          'enlace': artista['external_urls']['spotify'],
        }
      }

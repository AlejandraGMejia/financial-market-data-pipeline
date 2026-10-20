"""Configuración del proyecto."""

import os

from dotenv import load_dotenv

load_dotenv()


def get_api_key():
    """Obtener la API Key desde las variables de entorno."""

    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    if not api_key:
        raise ValueError("No se encontró la API Key")

    return api_key

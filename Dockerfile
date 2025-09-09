FROM astrocrpublic.azurecr.io/runtime:3.0-10

# Cambiar a usuario root para poder usar apt-get
USER root

# Instalar certificados raíz
RUN apt-get update && apt-get install -y ca-certificates
RUN update-ca-certificates

# Instalar dependencias del proyecto
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Volver al usuario original de Astronomer
USER astro
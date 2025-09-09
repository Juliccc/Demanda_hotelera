FROM astrocrpublic.azurecr.io/runtime:3.0-10

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install requests
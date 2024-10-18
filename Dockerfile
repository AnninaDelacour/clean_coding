FROM python:3.10-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app

RUN pip install dagster dagit dagster-webserver pandas pandera

# Kopiere den gesamten Inhalt des Ordners "elt_processing" ins Verzeichnis /opt/dagster/app
COPY . /opt/dagster/app/

# Setze das Dagster-Home-Verzeichnis
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

# Setze den PYTHONPATH, damit Python die Module finden kann
ENV PYTHONPATH=/opt/dagster/app

# Erstelle eine leere dagster.yaml-Datei im Dagster-Home-Verzeichnis
RUN touch /opt/dagster/dagster_home/dagster.yaml

# Arbeitsverzeichnis setzen
WORKDIR /opt/dagster/app

# Exponiere den Port 3000 für Dagit
EXPOSE 3000

# Starte den Dagster-Webserver
ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace.yaml"]

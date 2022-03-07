FROM python:3.7

RUN python3 -m venv /opt/venv

ENTRYPOINT bin/bash

RUN /bin/bash -c "source /opt/venv/bin/activate"

RUN pip install apache-beam[gcp]

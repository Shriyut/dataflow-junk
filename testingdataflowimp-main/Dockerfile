FROM python:3.7

RUN python3 -m venv /opt/venv

ENTRYPOINT bin/bash

COPY requirements.txt .

RUN /bin/bash -c "source /opt/venv/bin/activate"

RUN /bin/bash -c "pip3 install -r requirements.txt"

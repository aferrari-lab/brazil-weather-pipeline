# Imagem do job de previsão de temperatura: Python + scikit-learn + driver do Postgres.
# Separada das outras para cada etapa ter só as bibliotecas de que precisa.
FROM python:3.12-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN useradd --create-home --uid 1000 ml
USER ml

WORKDIR /opt/ml
COPY --chown=ml:ml previsao.py /opt/ml/previsao.py

ENTRYPOINT ["python", "previsao.py"]

FROM            python:3.7.1

RUN             mkdir /app
COPY            requirements.txt /app
RUN             pip install -r /app/requirements.txt
COPY            asyncy /app/asyncy

WORKDIR         /app
CMD             ["python", "-m", "asyncy.synapse.App"]

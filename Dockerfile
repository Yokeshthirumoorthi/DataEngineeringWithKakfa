FROM python:3.7-slim

WORKDIR /srv

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

COPY *.py ./

CMD ["python", "load_inserts.py"]
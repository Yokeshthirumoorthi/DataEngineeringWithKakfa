FROM python:3.7-slim

# ENV LIBRDKAFKA_VERSION 0.11.4
# RUN curl -Lk -o /root/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz && \
#     tar -xzf /root/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz -C /root && \
#     cd /root/librdkafka-${LIBRDKAFKA_VERSION} && \
#     ./configure && make && make install && make clean && ./configure --clean

# ENV CPLUS_INCLUDE_PATH /usr/local/include
# ENV LIBRARY_PATH /usr/local/lib
# ENV LD_LIBRARY_PATH /usr/local/lib

# RUN pip install confluent-kafka==0.11.4

WORKDIR /srv

COPY requirements.txt .
RUN pip3 install -U -r requirements.txt

COPY *.py ./
COPY librdkafka.config .

# CMD ["python", "data_gatherer.py"]
CMD ["python", "server.py"]


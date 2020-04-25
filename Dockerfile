# Cannot build from Alpine a lot of Kafka/Confluent libraries are not supported
FROM python:3.7.7

COPY . usr/src/kgai-py-crawler/
WORKDIR /usr/src/kgai-py-crawler

RUN pip install pipenv
RUN pipenv install

RUN chmod +x run_app.sh
CMD [ "sh", "./run_app.sh" ]
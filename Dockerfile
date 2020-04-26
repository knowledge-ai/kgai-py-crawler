# Cannot build from Alpine a lot of Kafka/Confluent libraries are not supported
FROM python:3.7.7

COPY . usr/src/kgai-py-crawler/
WORKDIR /usr/src/kgai-py-crawler

# running app in a virtualenv in docker is ironic
# the downsides (even if you do not care about the irony)
# are that you wont be able to write proper 12-factor apps
# like passing environment variable configs, the virtualenv
# in the container will not retain the env var
RUN pip install pipenv
RUN pipenv lock --requirements > requirements.txt
RUN pip install -r requirements.txt
RUN pipenv --rm
RUN pip uninstall -y pipenv


RUN chmod +x run_app.sh
CMD [ "sh", "./run_app.sh" ]
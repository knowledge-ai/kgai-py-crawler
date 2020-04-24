FROM python:3.7.7

COPY . usr/src/kgai-py-crawler/
WORKDIR /usr/src/kgai-py-crawler

RUN pip install pipenv
RUN pipenv install

# RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x run_app.sh
CMD [ "sh", "./run_app.sh" ]
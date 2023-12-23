FROM python:3.9.18

WORKDIR /strava_etl

COPY requirements.txt .
COPY ./src ./src
COPY ./tests ./tests
COPY ./configs ./configs
COPY ./logs ./logs
COPY ./test_data ./test_data

RUN pip install -r requirements.txt

CMD cd tests
CMD python tests/tests.py
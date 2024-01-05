FROM python:3.9.18

WORKDIR /strava_etl

COPY ./src ./src
COPY ./configs ./configs
COPY ./Pipfile ./Pipfile
COPY ./Pipfile.lock ./Pipfile.lock

ENV PYTHONPATH "${PYTHONPATH}:/strava_etl"

RUN pip install pipenv
RUN pipenv install --system --deploy --ignore-pipfile

CMD python src/main.py configs/configs.yml
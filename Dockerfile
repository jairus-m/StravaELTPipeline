FROM python:3.9.18

WORKDIR /strava_etl

COPY ./src ./src
COPY ./tests ./tests
COPY ./configs ./configs

ENV PYTHONPATH "${PYTHONPATH}:/strava_etl"

CMD pipenv shell && python src/main.py configs/configs.yml
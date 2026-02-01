FROM ghcr.io/dbt-labs/dbt-postgres:1.9.latest

WORKDIR /usr/app

COPY . /usr/app

RUN dbt deps

CMD ["build"]

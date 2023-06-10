FROM python:3.10

USER root

ARG run_date="now"
ENV run_date=${run_date}

WORKDIR app
COPY . /app

RUN echo "${run_date}"

RUN apt-get update

RUN pip3 install --upgrade setuptools wheel pip

RUN apt install -y build-essential python3-dev zlib1g-dev liblz4-dev

RUN python3 -m pip install -r requirements.txt

ENTRYPOINT python3 run.py --run-date ${run_date}

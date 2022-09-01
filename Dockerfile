FROM python:3.8-slim-buster

#RUN apt-get update -y && \
#    apt-get install -y python3-dev

WORKDIR /app

COPY . ./

RUN python -m pip install .

ENTRYPOINT ["python", "bin/block_crawler.py"]

CMD ["tail"]

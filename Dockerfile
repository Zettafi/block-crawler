FROM python:3.8-slim-buster

WORKDIR /app

COPY . ./

RUN python -m pip install .

ENTRYPOINT ["python", "-m", "blockcrawler"]

CMD ["nft", "tail"]

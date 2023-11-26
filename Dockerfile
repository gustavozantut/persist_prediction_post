FROM python:3.11
WORKDIR /usr/src/app
RUN apt-get update
COPY ./app ./
COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN rm requirements.txt
ENTRYPOINT ["python", "persist_post_predictions.py"]
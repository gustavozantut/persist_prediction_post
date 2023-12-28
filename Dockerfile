FROM python:3.11
RUN git clone https://github.com/gustavozantut/persist_prediction_post /usr/src/app/persist_prediction_post/
RUN rm /usr/src/app/persist_prediction_post/Dockerfile
WORKDIR /usr/src/app/persist_prediction_post
RUN apt-get update
RUN pip install -r ./requirements.txt
RUN rm ./requirements.txt
WORKDIR /usr/src/app/persist_prediction_post/app
ENTRYPOINT ["python", "persist_post_predictions.py"]
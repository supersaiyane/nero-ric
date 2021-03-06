FROM python:3.9-slim

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV  PORT 1708
WORKDIR $APP_HOME
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./main.py" ]

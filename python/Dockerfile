# NOTE name need to be Dockerfile
FROM python:3.9

RUN mkdir -p /usr/src/python-app
#RUN mkdir -p /usr/src/python-app/data/raw
#RUN mkdir -p /usr/src/python-app/data/work

WORKDIR /usr/src/python-app

COPY . /usr/src/python-app/

RUN ls
RUN ECHO 'TESTE DE requirements'
RUN pip install -r requirements.txt
# RUN pip install mysql-connector-python
# RUN pip install SQLAlchemy

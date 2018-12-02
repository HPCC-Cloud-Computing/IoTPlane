FROM ubuntu:16.04

MAINTAINER HaiQuan "haiquan5396@gmail.com"

ENV BROKER_CLOUD localhost

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

EXPOSE 5000

#ENTRYPOINT python3 API.py ${BROKER_CLOUD}

CMD python3 API.py ${BROKER_CLOUD}

#docker build -t haiquan5396/api:1.2 .
#docker run -it --name api -p 5000:5000 -e "BROKER_CLOUD=192.168.60.248" haiquan5396/api:1.2

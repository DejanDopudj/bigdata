FROM python:3.8

COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME /usr/local/openjdk-8

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1


WORKDIR .

COPY requirements.txt /requirements.txt


RUN pip install -r requirements.txt

COPY . .

COPY ./data /data


CMD ["python", "spark.py"]

FROM openjdk:12-oraclelinux7

RUN yum -y install \
    libXi \
   libXrender

RUN yum -y install libXtst

COPY . /src

WORKDIR /src

CMD java -jar project2.jar
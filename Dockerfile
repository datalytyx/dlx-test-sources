FROM mysql:5.7
#FROM ubuntu

RUN apt-get update && apt-get install -y --no-install-recommends \
      uuid-dev cron logrotate \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg-agent \
      vim \
      uuid \
      software-properties-common \
	locales \
      python3 python3-pip python3-dev

# This is all needed in python3 to avoid errors like: UnicodeDecodeError: 'ascii' codec can't decode byte 0xc2 in position
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8

RUN pip3 --no-cache-dir install --upgrade \
        setuptools \
        wheel

#RUN pip3 install --upgrade pip3 && 
RUN pip3 install --upgrade pip setuptools
RUN pip3 install jinja2 pyyaml pysed curlify

ADD mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf
ADD mysqld.cnf /etc/mysql/mysql.conf.d/mysqld.cnf2
ADD * /dlx-test-sources/
ADD mysql-flex /dlx-test-sources/mysql-flex
RUN mv /dlx-test-sources/mysql-flex/adventureworks/* /docker-entrypoint-initdb.d/

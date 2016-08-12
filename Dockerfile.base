# install os
FROM ubuntu:16.04

# install oracle java 
RUN \
  apt-get update && \
  apt-get install -y software-properties-common && \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# install python From FROM python:2.7 does not work. and conflict with Java8
RUN \
  apt-get update && \
  apt-get install -y python2.7 python2.7-dev python-pip python-setuptools

# install the supervisor
RUN easy_install supervisor
RUN mkdir /etc/supervisor/
ADD supervisord.conf /etc/supervisor/supervisord.conf

# install boto and producer
RUN pip install boto
RUN mkdir /usr/local/producer
ADD producer.conf /usr/local/producer/producer.conf
ADD producer.py /usr/local/producer/producer.py

# install code auto deploy
RUN wget https://github.com/wesley1975/codeautodeploy/raw/master/codeautodeploy.py -P /home
RUN chmod a+x /home/codeautodeploy.py
RUN mkdir /usr/local/codeautodeploy
RUN cp /home/codeautodeploy.py /usr/local/codeautodeploy/
ADD codeautodeploy.cfg /usr/local/codeautodeploy/


# set the mapper port
EXPOSE 8080

# run the init command
CMD /usr/local/bin/supervisord -n -c /etc/supervisor/supervisord.conf
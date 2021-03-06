#  Docker file for Ubuntu 16.04 w/ python 3.6
#  https://gist.github.com/monkut/c4c07059444fd06f3f8661e13ccac619
FROM ubuntu:16.04

#  Set the working directory
WORKDIR /app/transportation-data-publishing

#  Copy package requirements
COPY requirements.txt /app/transportation-data-publishing

RUN apt-get update

#  Set Timezone :(  
ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

#  Install cifs-utils to mount Windows network share
RUN apt-get install -y cifs-utils

# Install python support in vim
RUN apt-get install -y software-properties-common vim

#  Install Python 3.6 build for Ubuntu 16.04
RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update --fix-missing
RUN apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
RUN apt-get install -y git
RUN apt-get install -y iputils-ping

#  Required for pymssql
RUN apt-get update && apt-get install -y \
    freetds-bin \
    freetds-common \
    freetds-dev

#  Update python3-pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

#  Install python packages specified in requirements.txt
RUN pip3.6 install --trusted-host pypi.python.org -r requirements.txt

#  Link python path to python3.6
RUN cd /usr/local/bin && \
    ln -s /usr/bin/python3.6 python
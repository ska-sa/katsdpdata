FROM sdp-docker-registry.kat.ac.za:5000/psrchive:v3

MAINTAINER Thomas Bennett "tbennett@ska.ac.za"

# Suppress debconf warnings
ENV DEBIAN_FRONTEND noninteractive

# Install some system packages used by multiple images.
USER root
RUN apt-get -y update && apt-get -y install \
    libhdf5-dev  
USER kat

COPY requirements.txt /tmp/install/requirements.txt
#COPY install-requirements.py .
#RUN pip install pkginfo
#RUN pip install futures
#RUN pip install --upgrade pip
RUN install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt 

# Install the current package
COPY . /tmp/install/katsdpdata
WORKDIR /tmp/install/katsdpdata
RUN python ./setup.py clean && pip install --no-index .
#WORKDIR /var/kat/data

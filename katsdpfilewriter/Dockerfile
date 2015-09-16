FROM ubuntu:14.04

MAINTAINER Bruce Merry "bmerry@ska.ac.za"

# Suppress debconf warnings
ENV DEBIAN_FRONTEND noninteractive

# Set up access to github private repositories
COPY conf/id_rsa /root/.ssh/
RUN echo "Host *\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config
RUN chmod -R go-rwx ~/.ssh

# Install system packages
RUN apt-get -y update && apt-get -y install \
    build-essential git-core \
    libboost-python1.55-dev \
    libboost-system1.55-dev \
    python python-dev python-pip \
    python-h5py \
    python-netifaces \
    python-numpy \
    python-ply \
    python-twisted \
    python-zope.interface

RUN pip install --no-deps \
    katcp==0.5.5 \
    manhole \
    redis==2.10.3 \
    six==1.9.0 \
    spead2==0.2.2
COPY requirements.txt /tmp/install/requirements.txt
# Keep only dependent git repositories; everything else is installed explicitly
# by this Dockerfile.
RUN sed -n '/^git/p' /tmp/install/requirements.txt > /tmp/install/requirements-git.txt && \
    pip install --no-deps -r /tmp/install/requirements-git.txt

# Install the current package
COPY . /tmp/install/katsdpingest
WORKDIR /tmp/install/katsdpingest
RUN python ./setup.py clean && pip install --no-deps .

# Run ingest as a non-root user
RUN adduser --system kat
WORKDIR /home/kat
USER kat

FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Bruce Merry "bmerry@ska.ac.za"

COPY requirements.txt /tmp/install/requirements.txt
RUN cd /home/kat && \
    git clone --recursive git+ssh://git@github.com/ska-sa/spead2 && \
    cd spead2 && \
    git reset --hard 10143470725afcd9cc5f5d0741e8dec2e48a7e4c && \
    git submodule update && \
    ./bootstrap.sh && pip install .
RUN install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt 

# Install the current package
COPY . /tmp/install/katsdpfilewriter
WORKDIR /tmp/install/katsdpfilewriter
RUN python ./setup.py clean && pip install --no-index .

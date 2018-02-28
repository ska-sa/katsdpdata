FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Simon Ratcliffe "simonr@ska.ac.za"

COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpflagwriter
WORKDIR /tmp/install/katsdpflagwriter
RUN python ./setup.py clean && pip install --no-deps . && pip check

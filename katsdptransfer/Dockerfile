FROM sdp-ingest5.kat.ac.za:5000/docker-base

MAINTAINER Thomas Bennett "tbennett@ska.ac.za"

# Install the current package
COPY . /tmp/install/katsdptransfer
WORKDIR /tmp/install/katsdptransfer
RUN python ./setup.py clean && pip install --no-index .

VOLUME ["/var/kat/data"]

#--path /var/kat/data/staging
#--on_success /var/kat/data/success


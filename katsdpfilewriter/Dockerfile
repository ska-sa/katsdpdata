FROM sdp-docker-registry.kat.ac.za:5000/docker-base-build as build
MAINTAINER Bruce Merry "bmerry@ska.ac.za"

# Activate Python 2 ve
ENV PATH="$PATH_PYTHON2" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON2"

# Install dependencies
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpfilewriter
WORKDIR /tmp/install/katsdpfilewriter
RUN python ./setup.py clean
RUN pip install --no-deps .
RUN pip check

#######################################################################

FROM sdp-docker-registry.kat.ac.za:5000/docker-base-runtime
MAINTAINER Bruce Merry "bmerry@ska.ac.za"

COPY --from=build --chown=kat:kat /home/kat/ve /home/kat/ve
ENV PATH="$PATH_PYTHON2" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON2"

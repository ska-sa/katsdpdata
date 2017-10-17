FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Bruce Merry "bmerry@ska.ac.za"

COPY requirements.txt /tmp/install/requirements.txt
RUN install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt
RUN cd /home/kat && \
    git clone --recursive git+ssh://git@github.com/ska-sa/spead2 && \
    cd spead2 && \
    git reset --hard c84ed1346a225fa2455f024afb46120a2aa2c9f4 && \
    git submodule update && \
    ./bootstrap.sh && pip uninstall -y spead2 && pip install .

# Install the current package
COPY . /tmp/install/katsdpfilewriter
WORKDIR /tmp/install/katsdpfilewriter
RUN python ./setup.py clean && pip install --no-index .

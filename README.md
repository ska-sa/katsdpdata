katsdpdata
==========

Data search and retrieval library for the MeerKAT project.

Must build the psrchive docker before building the katsdpdata docker as it
is the base

psrchive_docker
===============
Docker image containing the software necessary to open and read beamformer data

Build this as sdp-docker-registry.kat.ac.za:5000/psrchive if you
need it to work with the katsdpdata Dockerfile

# Copyright (C) 2016 by Maciej Serylak
# Licensed under the Academic Free License version 3.0
# This program comes with ABSOLUTELY NO WARRANTY.
# You are free to modify and redistribute this code as long
# as you do not remove the above attribution and reasonably
# inform receipients that you have modified the original work.


FROM sdp-docker-registry.kat.ac.za:5000/docker-base-gpu-build:latest


MAINTAINER Christopher Schollar "cscollar@ska.ac.za"


# Switch account to root and adding user accounts and password
USER root
RUN echo "kat:kat" | chpasswd
RUN echo "root:Docker!" | chpasswd


# Create space for ssh deamon and update the system
RUN mkdir /var/run/sshd && \
    apt-get -y check && \
    apt-get -y update && \
    apt-get -y update --fix-missing && \
    apt-get -y upgrade


# Install system packages
RUN apt-get -y install \
    apt-utils \
    autoconf \
    automake \
    autotools-dev \
    binutils-dev \
    bison \
    build-essential \
    cmake \
    cmake-curses-gui \
    cmake-data \
    cpp \
    csh \
    cvs \
    cython \
    dkms \
    f2c \
    fftw-dev \
    fftw2 \
    flex \
    fort77 \
    g++ \
    gawk \
    gcc \
    gfortran \
    ghostscript \
    ghostscript-x \
    git \
    git-core \
    gnuplot \
    gnuplot-x11 \
    gsl-bin \
    gv \
    h5utils \
    hdf5-tools \
    hdfview \
    htop \
    hwloc \
    ipython \
    ipython-notebook \
    libatlas-dev \
    libbison-dev \
    libblas-dev \
    libblas3 \
    #libblas3gf \
    libboost-program-options1.58-dev \
    libboost-python1.58-dev \
    libboost-regex1.58-dev \
    libboost-system1.58-dev \
    libboost1.58-all-dev \
    libboost1.58-dev \
    libboost1.58-tools-dev \
    libc-dev-bin \
    libc6-dev \
    #libcfitsio3 \
    libcfitsio3-dev \
    libcloog-isl4 \
    libcppunit-dev \
    libcppunit-subunit-dev \
    libcppunit-subunit0 \
    libfftw3-3 \
    libfftw3-bin \
    libfftw3-dbg \
    libfftw3-dev \
    libfftw3-double3 \
    libfftw3-long3 \
    libfftw3-quad3 \
    libfftw3-single3 \
    libfreetype6 \
    libfreetype6-dev \
    libgd-dev \
    libgd2-xpm-dev \
    libgd3 \
    libglib2.0-0 \
    libglib2.0-dev \
    libgmp3-dev \
    libgsl-dev \
    libgsl-dbg \
    libgtksourceview-3.0-dev \
    libgtksourceview2.0-dev \
    libhdf5-10 \
    libhdf5-dev \
    libhdf5-serial-dev \
    libhdf5-10 \
    libhdf5-dev \
    libhdf5-serial-dev \
    libhwloc-dev \
    liblapack-dev \
    liblapack-pic \
    liblapack-test \
    liblapack3 \
    liblapacke \
    liblapacke-dev \
    libltdl-dev \
    libltdl7 \
    liblua5.1-0 \
    liblua5.1-0-dev \
    liblua5.2-0 \
    liblua5.2-dev \
    libncurses5-dev \
    libopenblas-base \
    libopenblas-dev \
    libpng++-dev \
    libpng-sixlegs-java \
    libpng-sixlegs-java-doc \
    libpng12-0 \
    libpng12-dev \
    libpng3 \
    libpnglite-dev \
    libpth-dev \
    libreadline6 \
    libreadline6-dev \
    libsocket++-dev \
    libsocket++1 \
    libssl-dev \
    libtool \
    libx11-dev \
    llvm-3.8 \
    llvm-3.8-dev \
    llvm-3.8-doc \
    llvm-3.8-examples \
    llvm-3.8-runtime \
    locate \
    lsof \
    m4 \
    make \
    man \
    mc \
    nano \
    nfs-common \
    numactl \
    openssh-server \
    pbzip2 \
    pgplot5 \
    pkg-config \
    pkgconf \
    pyqt4-dev-tools \
    python \
    python-dev \
    python-pip \
    python-qt4-dev \
    python-tk \
    screen \
    subversion \
    #swig \
    swig2.0 \
    tcsh \
    tk \
    tk-dev \
    tmux \
    vim \
    wcslib-dev \
    wcslib-tools \
    wget \
    wget \
    zlib1g-dev


# Switch account to kat
USER kat


# Define home, psrhome, OSTYPE and create the directory
ENV HOME /home/kat
ENV PSRHOME /home/kat/software
ENV OSTYPE linux
RUN mkdir -p /home/kat/software


# Install python modules
ENV PATH $PATH_PYTHON2
ENV VIRTUAL_ENV $VIRTUAL_ENV_PYTHON2
RUN pip install pip -U && \
    pip install setuptools -U && \
    pip install ipython -U && \
    pip install jupyter -U && \
    pip install six -U && \
    pip install numpy -U && \
    pip install scipy -U && \
    pip install runipy -U && \
    pip install matplotlib -U


# Python packages
ENV PYTHONPATH $HOME/ve/lib/python2.7/site-packages


# Putting symlink to libpython so PSRCHIVE configure script can find it
RUN ln -s /usr/lib/x86_64-linux-gnu/libpython2.7.so $HOME/ve/lib/python2.7/libpython2.7.so


# Downloading all source codes
WORKDIR $PSRHOME
RUN wget --no-check-certificate https://www.imcce.fr/content/medias/recherche/equipes/asd/calceph/calceph-3.3.1.tar.gz && \
    tar -xvvf calceph-3.3.1.tar.gz -C $PSRHOME && \
    wget --no-check-certificate http://www.atnf.csiro.au/people/pulsar/psrcat/downloads/psrcat_pkg.tar.gz && \
    tar -xvf psrcat_pkg.tar.gz -C $PSRHOME && \
    git clone --progress https://github.com/straten/epsic.git && \
    git clone --progress https://bitbucket.org/psrsoft/tempo2.git && \
    git clone --progress git://git.code.sf.net/p/tempo/tempo && \
    git clone --progress https://github.com/SixByNine/psrxml.git && \
    git clone --progress git://git.code.sf.net/p/psrchive/code psrchive


# PGPLOT
ENV PGPLOT_DIR /usr/lib/pgplot5
ENV PGPLOT_FONT /usr/lib/pgplot5/grfont.dat
ENV PGPLOT_INCLUDES /usr/include
ENV PGPLOT_BACKGROUND white
ENV PGPLOT_FOREGROUND black
ENV PGPLOT_DEV /xs


# calceph
ENV CALCEPH $PSRHOME/calceph-3.3.1
ENV PATH $PATH:$CALCEPH/install/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$CALCEPH/install/lib
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$CALCEPH/install/include
WORKDIR $CALCEPH
RUN ./configure --prefix=$CALCEPH/install --with-pic --enable-shared --enable-static --enable-fortran --enable-thread && \
    make -j $(nproc) && \
    make check && \
    make install && \
    make clean


# psrcat
ENV PSRCAT_FILE $PSRHOME/psrcat_tar/psrcat.db
ENV PATH $PATH:$PSRHOME/psrcat_tar
WORKDIR $PSRHOME/psrcat_tar
RUN /bin/bash makeit && \
    rm -rf *.o


# tempo
ENV TEMPO $PSRHOME/tempo
ENV PATH $PATH:$PSRHOME/tempo/bin
WORKDIR $PSRHOME/tempo
RUN ./prepare && \
    ./configure --prefix=$PSRHOME/tempo && \
    make -j $(nproc)&& \
    make install && \
    make clean && \
    echo " 5109318.8410  2006836.3673    -3238921.7749   1  MEERKAT             m  MK" >> obsys.dat && \
    awk '{print $(NF-1), $0}' obsys.dat | sort -V | cut -d\  -f2-


# tempo2
ENV TEMPO2 $PSRHOME/tempo2/T2runtime
ENV PATH $PATH:$PSRHOME/tempo2/T2runtime/bin
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$PSRHOME/tempo2/T2runtime/include
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$PSRHOME/tempo2/T2runtime/lib
WORKDIR $PSRHOME/tempo2
RUN sync && perl -pi -e 's/chmod \+x/#chmod +x/' bootstrap # Get rid of: returned a non-zero code: 126.
RUN ./bootstrap && \
    ./configure --x-libraries=/usr/lib/x86_64-linux-gnu --with-calceph=$CALCEPH/install/lib --enable-shared --enable-static --with-pic F77=gfortran CPPFLAGS="-I"$CALCEPH"/install/include" && \
    make -j $(nproc) && \
    make install && \
    make plugins-install && \
    make clean


# PSRXML
ENV PSRXML $PSRHOME/psrxml
ENV PATH $PATH:$PSRXML/install/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$PSRXML/install/lib
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$PSRXML/install/include
WORKDIR $PSRXML
RUN autoreconf --install --warnings=none
RUN ./configure --prefix=$PSRXML/install && \
    make -j $(nproc) && \
    make install && \
    make clean


# epsic
ENV EPSIC $PSRHOME/epsic
ENV PATH $PATH/$EPSIC/install/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH/$EPSIC/install/lib
ENV C_INCLUDE_PATH $C_INCLUDE_PATH/$EPSIC/install/include
WORKDIR $EPSIC/src
RUN ./bootstrap && \
    ./configure --prefix=$EPSIC/install --enable-shared && \
    make -j $(nproc) && \
    make && \
    make install && \
    make clean


# PSRCHIVE
ENV PSRCHIVE $PSRHOME/psrchive
ENV PATH $PATH:$PSRCHIVE/install/bin
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$PSRCHIVE/install/include
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$PSRCHIVE/install/lib
ENV PYTHONPATH $PYTHONPATH:$PSRCHIVE/install/lib/python2.7/site-packages
WORKDIR $PSRCHIVE
RUN ./bootstrap && \
    ./configure --prefix=$PSRCHIVE/install --x-libraries=/usr/lib/x86_64-linux-gnu --with-psrxml-dir=$PSRXML/install --enable-shared --enable-static --disable-psrhome --with-epsic-lib-dir=$EPSIC/install/lib --with-epsic-dir=$EPSIC/install --with-epsic-include-dir=$EPSIC/install/include F77=gfortran LDFLAGS="-L$PSRXML/install/lib -L$EPSIC/install/lib" CPPFLAGS="-I$EPSIC/install" LIBS="-lpsrxml -lxml2 -lreadline -lepsic" PGPLOT_DIR=/usr/lib/pgplot5 && \
    make -j $(nproc) && \
    make && \
    make install && \
    make clean
RUN echo "meerkat m" >> $PSRHOME/tempo2/T2runtime/observatory/aliases
WORKDIR $HOME
RUN $PSRCHIVE/install/bin/psrchive_config >> .psrchive.cfg && \
    sed -i 's/# ArrivalTime::default_format = Parkes/ArrivalTime::default_format = Tempo2/g' .psrchive.cfg && \
    sed -i 's/# Predictor::default = polyco/Predictor::default = tempo2/g' .psrchive.cfg && \
    sed -i 's/# Predictor::policy = ephem/Predictor::policy = default/g' .psrchive.cfg && \
    sed -i 's/# FrequencyAppend::force_new_predictor = 0/# FrequencyAppend::force_new_predictor = 1/g' .psrchive.cfg && \
    sed -i 's/# WeightedFrequency::round_to_kHz = 1/WeightedFrequency::round_to_kHz = 0/g' .psrchive.cfg && \
    sed -i 's/# DeleteInterpreter::adjust_metadata_while_deleting_channels = 0/DeleteInterpreter::adjust_metadata_while_deleting_channels = 1/g' .psrchive.cfg && \
    sed -i 's/# Dispersion::barycentric_correction = 0/Dispersion::barycentric_correction = 1/g' .psrchive.cfg


# clean downloaded source codes
WORKDIR $PSRHOME
RUN rm -rf ./*.gz


# Update database for locate
USER root
RUN updatedb && \
    ldconfig

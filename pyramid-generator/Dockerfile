FROM amazonlinux:2018.03.0.20180424

# stuff we need to build our own libvips 
# this is a pretty minimal selection of dependencies
# especially consider orc, the runtime compiler, it can give libvips a 
# good speedup on many operations
RUN yum install -y \
  wget \
  tar \
  gcc-c++ \
	glib2-devel \
	expat-devel \
	zlib-devel \
	libpng-devel \
	libjpeg-devel \
	libtiff-devel \
  gzip \
  make \
  glibc-devel \
  patch \
  findutils \
  gcc-c++ \
  pkgconig \
  glib2-devel \
  expat-devel \
  autoconf \
  automake \
  libtool \
  gtk-doc \
  gobject-introspection-devel \
  libjpeg-devel \
  libtiff-devel \
  libjpeg-turbo-devel \
  libgsf-devel

ARG VIPS_VERSION=8.7.0
ARG VIPS_URL=https://github.com/jcupitt/libvips/archive

# the gcc7 that amazon defaults to has problems with mixed scalar and vector
# arithmetic ... we have to build with 4.8
# we need to ldconfig to get the loader to see the new library in /usr/lib
RUN cd /usr/local/src \
	&& wget ${VIPS_URL}/v${VIPS_VERSION}.tar.gz \
	&& tar xzf v${VIPS_VERSION}.tar.gz \
	&& cd libvips-${VIPS_VERSION} \
	&& CC="gcc48 -O3" CXX="g++48 -O3" ./autogen.sh --prefix=/usr \
	&& make V=0 \
	&& make install \
	&& ldconfig

# install latest pip
RUN cd /usr/local/src \
	&& curl -O https://bootstrap.pypa.io/get-pip.py \
	&& python get-pip.py

# install pip pkgs
RUN pip install boto3 pyvips

COPY pyramid.py /usr/local/bin/
ENTRYPOINT ["pyramid.py"]

#WORKDIR /data
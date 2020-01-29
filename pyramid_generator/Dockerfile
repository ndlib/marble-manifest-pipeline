FROM alpine:latest

RUN apk update && apk upgrade

# basic packages libvips likes
RUN apk add \
	build-base \
	autoconf \
	automake \
	libtool \
	bc \
	zlib-dev \
	expat-dev \
	jpeg-dev \
	tiff-dev \
	glib-dev \
	libjpeg-turbo-dev \
	libexif-dev \
	lcms2-dev \
	fftw-dev \
	giflib-dev \
	libpng-dev \
	libwebp-dev \
	orc-dev \
	libgsf-dev 

# text rendering, PDF rendering, SVG rendering
RUN apk add \
	gdk-pixbuf-dev \
	poppler-dev \
	librsvg-dev 

# install vips
ARG VIPS_VERSION=8.8.3
ARG VIPS_URL=https://github.com/libvips/libvips/releases/download

RUN wget -O- ${VIPS_URL}/v${VIPS_VERSION}/vips-${VIPS_VERSION}.tar.gz | tar xzC /tmp
RUN cd /tmp/vips-${VIPS_VERSION} \
	&& ./configure --prefix=/usr --disable-static --disable-debug \
	&& make V=0 \
	&& make install

RUN apk add \
	python3-dev \
	py3-pip

# and now pyvips can go on
RUN pip3 install --upgrade pip \
  && pip3 install pyvips boto3

# image transformation script
COPY pyramid.py /usr/local/bin/
COPY image_processor.py /usr/local/bin/
COPY pipeline_config.py /usr/local/bin/
COPY csv_collection.py /usr/local/bin/
RUN chmod u+x /usr/local/bin/pyramid.py
#ENTRYPOINT ["pyramid.py"]

FROM osgeo/gdal:ubuntu-small-latest

WORKDIR /usr/src/app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY . .

RUN apt update\
    && apt install -y \
        libmagic-dev \
        git

RUN pip3 install -r requirements.txt

CMD ["./entrypoint.sh"]
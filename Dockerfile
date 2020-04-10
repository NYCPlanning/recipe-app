FROM osgeo/gdal:ubuntu-small-latest

WORKDIR /usr/src/app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ARG PORT=5000

COPY . .

RUN apt update\
    && apt install -y \
        libmagic-dev \
        git\
        python3-pip

RUN pip install -r requirements.txt

CMD ["./entrypoint.sh"]
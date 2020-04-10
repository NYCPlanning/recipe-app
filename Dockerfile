FROM nycplanning/cook:latest

WORKDIR /usr/src/app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ARG PORT=5000

COPY . .

RUN apt update\
    && apt install -y \
        libmagic-dev \
        git\
        python3-pip\ 
        build-essential\
        libffi-dev\
    && apt autoclean

RUN pip3 install -r requirements.txt

CMD ["./entrypoint.sh"]

EXPOSE 5000
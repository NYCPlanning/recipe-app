FROM nycplanning/cook:latest

WORKDIR /usr/src/app

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY . .

RUN pip3 install -r requirements.txt

CMD ["./entrypoint.sh"]

EXPOSE 5000
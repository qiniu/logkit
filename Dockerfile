FROM buildpack-deps:jessie-curl
RUN wget http://op26gaeek.bkt.clouddn.com/logkit.tar.gz && tar xvf logkit.tar.gz && rm logkit.tar.gz
RUN mkdir /app
RUN mkdir /logs
RUN mv _package_linux64/* /app
VOLUME /app/confs
VOLUME /logs
EXPOSE 3000
WORKDIR /app
ENTRYPOINT ["/app/logkit","-f","logkit.conf"]
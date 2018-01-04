FROM buildpack-deps:jessie-curl
ENV LOGKIT_VERSION nightly
RUN wget https://pandora-dl.qiniu.com/logkit_${LOGKIT_VERSION}.tar.gz && tar xvf logkit_${LOGKIT_VERSION}.tar.gz && rm logkit_${LOGKIT_VERSION}.tar.gz
RUN mkdir /app
RUN mkdir /logs
RUN mv _package_linux64_${LOGKIT_VERSION}/* /app
RUN sed -i -- 's/localhost//g' /app/logkit.conf
VOLUME /app/confs
VOLUME /logs
EXPOSE 3000
WORKDIR /app
ENTRYPOINT ["/app/logkit","-f","logkit.conf"]

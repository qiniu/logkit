FROM buildpack-deps:jessie-curl
ENV LOGKIT_VERSION v1.3.6
RUN wget https://pandora-dl.qiniu.com/logkit_${LOGKIT_VERSION}.tar.gz && tar xvf logkit_${LOGKIT_VERSION}.tar.gz && rm logkit_${LOGKIT_VERSION}.tar.gz
RUN mkdir /app
RUN mkdir /logs
RUN rm -r _package_linux64/template_confs
RUN mv _package_linux64/* /app
RUN sed -i -- 's/localhost//g' /app/logkit.conf
VOLUME /app/confs
VOLUME /logs
EXPOSE 3000
WORKDIR /app
ENTRYPOINT ["/app/logkit","-f","logkit.conf"]

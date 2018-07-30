FROM buildpack-deps:jessie-curl
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
ENV LOGKIT_VERSION nightly
RUN wget https://pandora-dl.qiniu.com/logkit_${LOGKIT_VERSION}.tar.gz && tar xvf logkit_${LOGKIT_VERSION}.tar.gz && rm logkit_${LOGKIT_VERSION}.tar.gz
RUN mkdir /app
RUN mkdir /logs
RUN mv _package_linux64_${LOGKIT_VERSION}/public /app/public
RUN mv _package_linux64_${LOGKIT_VERSION}/logkit /app/logkit
RUN mv _package_linux64_${LOGKIT_VERSION}/logkit.conf /app/logkit.conf

RUN sed -i -- 's/localhost//g' /app/logkit.conf
VOLUME /app/confs
VOLUME /logs
VOLUME /app/meta
VOLUME /app/.logkitconfs
EXPOSE 3000
WORKDIR /app
ENTRYPOINT ["/app/logkit","-f","logkit.conf"]

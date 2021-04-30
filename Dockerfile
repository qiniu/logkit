FROM buildpack-deps:jessie-curl
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && mkdir /app && mkdir /logs
ADD public /app/public
ADD logkit /app/logkit
ADD logkit.conf /app/logkit.conf

RUN sed -i -- 's/localhost//g' /app/logkit.conf
VOLUME /app/confs
VOLUME /logs
VOLUME /app/meta
VOLUME /app/.logkitconfs
EXPOSE 3000
WORKDIR /app
ENTRYPOINT ["/app/logkit","-f","logkit.conf"]

FROM registry.datayes.com/base/java:8u102-latest

ARG VERSION

ENV wd=/datayes/applog-flume



#用 ADD 会自动解压到目标目录
ADD ./target/applog-flume-${VERSION}.tar.gz ${wd}/applog-flume-${VERSION}.tar.gz

ADD ./docker/entrypoint.sh ${wd}/entrypoint.sh

WORKDIR ${wd}

RUN mkdir conf

RUN chmod 777 ${wd}/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
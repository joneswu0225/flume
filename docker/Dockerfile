FROM registry.datayes.com/base/java:8u102-latest

ARG VERSION

ENV wd=/datayes/applog-flume

WORKDIR ${wd}

#用 ADD 会自动解压到目标目录
ADD ./target/applog-flume-${VERSION}.zip ${wd}/

RUN unzip ${wd}/applog-flume-${VERSION}.zip -d ${wd}/

ADD ./docker/entrypoint.sh ${wd}/

RUN mkdir conf

RUN chmod 777 ${wd}/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
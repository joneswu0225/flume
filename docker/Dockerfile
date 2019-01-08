FROM registry.datayes.com/base/java:8u102-latest

ARG VERSION

ARG wd=/paas/applog-flume

#用 ADD 会自动解压到目标目录
ADD ./target/applog-flume-${VERSION}.tar.gz ${wd}/

ADD ./docker/entrypoint.sh ${wd}/

WORKDIR ${wd}

ENTRYPOINT ["./entrypoint.sh"]
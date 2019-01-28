FROM registry-ci.datayes.com/base/flume:1.8.0-1

ARG VERSION

ENV wd=/datayes/applog-flume

#用 ADD 会自动解压到目标目录
ADD ./target/applog-flume-${VERSION}.tar.gz ${wd}/

ADD ./docker/entrypoint.sh ${wd}/entrypoint.sh

WORKDIR ${wd}

RUN chmod 777 ${wd}/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
# Docker Image里面mvn构建太慢了，网速硬伤，直接在本地吧Scala项目打成jar放入镜像

FROM pharbers/spark-2.3.0-bin-hadoop2.7:1.1

RUN apt-get update && \
	apt-get install -f -y python python-pip && \
    apt-get clean

RUN useradd --create-home --no-log-init --shell /bin/bash bpstream
RUN chown bpstream $HADOOP_HOME && chown bpstream $SPARK_HOME

USER bpstream
WORKDIR /home/bpstream

# 先在本地打包在 COPY 到 Image
COPY jars/ ./jars/
COPY target/BP-Stream-Engine-1.0-SNAPSHOT.jar  .

ENTRYPOINT ["/bin/bash", "-c"]


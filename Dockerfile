FROM google/cloud-sdk:317.0.0

WORKDIR /root/

# add repo tools
RUN apt-get update && \
    apt-get --yes install \
    wget=1.20.1-1.1 \
    zlib1g-dev=1:1.2.11.dfsg-1 \
    openjdk-11-jre-headless=11.0.12+7-2~deb10u1

# add non-repo tools
RUN wget -qO- https://github.com/lh3/bwa/releases/download/v0.7.17/bwa-0.7.17.tar.bz2 | tar xjf - \
    && cd bwa-0.7.17 \
    && make \
    && cp bwa ../bwa \
    && cd .. \
    && chmod +x bwa \
    && rm -r bwa-0.7.17
RUN wget -qO- https://github.com/biod/sambamba/releases/download/v0.6.8/sambamba-0.6.8-linux-static.gz | gunzip -c > sambamba \
    && chmod +x sambamba
RUN git clone https://github.com/Daniel-Liu-c0deb0t/UMICollapse.git \
    && cd UMICollapse \
    && mkdir lib \
    && cd lib \
    && curl -O -L https://repo1.maven.org/maven2/com/github/samtools/htsjdk/2.19.0/htsjdk-2.19.0.jar \
    && curl -O -L https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.7.3/snappy-java-1.1.7.3.jar \
    && cd ../.. \
    && chmod +x UMICollapse/umicollapse

# add code
ADD src src
RUN find src -type f -exec chmod +x {} \;

ENTRYPOINT ["./src/do_analysis"]
FROM golang:1.17
COPY . /usr/src/collector/
WORKDIR /usr/src/collector/
RUN make install-tools
RUN make otelcontribcol
CMD [ "/usr/src/collector/bin/otelcontribcol_linux_amd64", "--config", "/usr/src/collector/local/config.yaml"]
EXPOSE 4317
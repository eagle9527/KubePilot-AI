FROM alpine:3.22 AS basefiles

RUN apk add --no-cache tzdata ca-certificates


FROM scratch

ENV TZ=Asia/Shanghai

COPY --from=basefiles /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=basefiles /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=basefiles /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt


COPY bin/manager /manager

USER 65532:65532

ENTRYPOINT ["/manager"]

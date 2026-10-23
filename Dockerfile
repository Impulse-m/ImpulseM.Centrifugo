FROM alpine:3.24

ARG USER=centrifugo
ARG UID=1000
ARG GID=1000

RUN addgroup -S -g $GID $USER && \
    adduser -S -G $USER -u $UID $USER

RUN apk --no-cache upgrade && \
    apk --no-cache add ca-certificates && \
    update-ca-certificates

USER $USER

WORKDIR /centrifugo

COPY centrifugo /usr/local/bin/centrifugo

COPY LICENSE NOTICE THIRD_PARTY_NOTICES.md /usr/share/doc/centrifugo/
COPY licenses /usr/share/doc/centrifugo/licenses

CMD ["centrifugo"]

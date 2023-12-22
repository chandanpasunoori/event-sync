FROM --platform=$TARGETPLATFORM alpine:3.9
RUN apk --no-cache add ca-certificates
RUN apk --no-cache add tzdata
COPY event-sync /
ENTRYPOINT ["/event-sync"]
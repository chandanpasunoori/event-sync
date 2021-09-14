FROM alpine:latest as alpine_builder
RUN apk --no-cache add ca-certificates
RUN apk --no-cache add tzdata
FROM golang:latest as builder
WORKDIR /app
COPY *.mod *.sum ./
RUN go mod download
COPY main.go .
CMD cmd pkg ./
RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build
FROM scratch
COPY --from=alpine_builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=alpine_builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /app/event-sync /event-sync
ENV TZ=Asia/Kolkata
ENTRYPOINT ["/event-sync"]
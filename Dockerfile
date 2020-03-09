# step: build goproxy
FROM golang:1.14-alpine3.11 as builder

COPY . /go/src/cow

RUN cd /go/src/cow &&\
    go build -o /app/cow

# step: run
FROM alpine:3.11
LABEL maintainer="dongdongking008 <dongdongking008@163.com>"

COPY --from=builder /app /app

WORKDIR /app

ENTRYPOINT ["/app/cow"]
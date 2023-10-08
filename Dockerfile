FROM golang:1.19.1-alpine

RUN apk update && apk add git

RUN mkdir -p /go/src/github.com/gorogoroumaru/godb

WORKDIR /go/src/github.com/gorogoroumaru/godb

ADD . /go/src/github.com/gorogoroumaru/godb


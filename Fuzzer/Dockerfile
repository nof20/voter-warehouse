FROM golang:1.8 as build-env

WORKDIR /go
ENV GOPATH=/go
ADD src/ /go/src/

RUN go get -d ./src/...
RUN go build ./src/...

RUN ls /go/bin

FROM gcr.io/distroless/base
COPY --from=build-env /go/bin/app /
CMD ["/app"]

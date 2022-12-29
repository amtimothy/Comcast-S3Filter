# syntax=docker/dockerfile:1

# parent image
FROM golang:1.19-alpine AS builder

# workspace directory
WORKDIR /app

# copy `go.mod` and `go.sum`
COPY go.mod ./
COPY go.sum ./
# install dependencies
RUN go mod download

# copy go source code
COPY *.go ./

# build executable
RUN go build -o ./bin/s3filter .

##################################

# parent image
FROM alpine

# workspace directory
WORKDIR /app

# copy binary file from the `builder` stage
COPY --from=builder /app/bin/s3filter ./

# set entrypoint
ENTRYPOINT [ "./s3filter" ]

# default cmd arg
CMD [ "-input" ]
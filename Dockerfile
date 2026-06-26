FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET=api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/service ./cmd/${TARGET}

FROM alpine:3.20
RUN apk add --no-cache tzdata && adduser -D -u 10001 powersight
USER powersight
WORKDIR /app
COPY --from=build /out/service /app/service
ENTRYPOINT ["/app/service"]

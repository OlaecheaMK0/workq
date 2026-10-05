FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /workq ./cmd/workq

FROM alpine:3.22
RUN addgroup -g 10001 app && adduser -D -u 10001 -G app app
COPY --from=build /workq /usr/local/bin/workq
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["workq"]
CMD ["api"]

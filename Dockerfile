FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN go build -o /bin/settlement-node ./cmd/node

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /bin/settlement-node /bin/settlement-node
ENTRYPOINT ["/bin/settlement-node"]

# Builds one command from cmd/, selected with the CMD build argument:
#
#   docker build -f deploy/Dockerfile --build-arg CMD=ingest -t threadroute-ingest .
#
# The build context is the repository root.

FROM golang:1.27.4-alpine AS build
WORKDIR /src

# Dependencies change less often than the code, so they get their own layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd/ cmd/
COPY internal/ internal/

ARG CMD=ingest
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -d "cmd/${CMD}" \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app "./cmd/${CMD}"

# distroless/static has CA certificates (ingest dials wss), time zone data
# and a non-root user, and no shell or package manager.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]

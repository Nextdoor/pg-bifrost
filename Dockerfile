# Download CA root certificates
FROM public.ecr.aws/docker/library/alpine:latest as certs
RUN apk --update add ca-certificates

# Test and build binary
FROM public.ecr.aws/docker/library/golang:1.22.3-bookworm as intermediate

# Make a directory to place pprof files in. Typically used for itests.
RUN mkdir /perf

# Build/test dependencies
RUN go install github.com/golang/mock/mockgen@v1.6.0
RUN go install golang.org/x/tools/cmd/goimports@latest

WORKDIR /go/src/github.com/Nextdoor/pg-bifrost.git/


# Copy over go modules and get dependencies. This will ensure
# that we don't get the deps each time but only when the files
# change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# The CI flag is used to control the auto generation of
# code from interfaces (running go generate). In dev we
# want that to happen automatically but in the CI build
# we only want to use the code that was checked in. When
# CI=true generate is not run.
ARG is_ci
ENV CI=$is_ci

# Run tests (if in CI build) then make the binary
RUN test -z "$CI" || make test
RUN make build

# Package binary & certs in a scratch container
FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=intermediate /perf /perf
COPY --from=intermediate /go/src/github.com/Nextdoor/pg-bifrost.git/target/pg-bifrost /
CMD ["/pg-bifrost"]

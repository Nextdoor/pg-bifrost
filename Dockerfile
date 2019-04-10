# Download CA root certificates
FROM alpine:latest as certs
RUN apk --update add ca-certificates

# Test and build binary
FROM golang:1.11.4-stretch as intermediate

# Make a directory to place pprof files in. Typically used for itests.
RUN mkdir /perf

# Build dependencies
RUN go get golang.org/x/tools/go/packages
RUN go install golang.org/x/tools/go/packages
RUN go get github.com/golang/mock/gomock
RUN go install github.com/golang/mock/mockgen
RUN go get github.com/golang/dep/cmd/dep
RUN go install github.com/golang/dep/cmd/dep

WORKDIR /go/src/github.com/Nextdoor/pg-bifrost.git/

# Copy over gopkg and get deps. This will ensure that
# we don't get the deps each time but only when the files
# change.
COPY Gopkg.lock Gopkg.toml ./
RUN dep ensure --vendor-only

COPY . .

# The CI flag is used to control the auto generation of
# code from interfaces (running go generate). In dev we
# want that to happen automatically but in the CI build
# we only want to use the code that was checked in. When
# CI=true generate is not run.
ARG is_ci
ENV CI=$is_ci

# Run tests and make the binary
# TODO(nehalrp): put this back before merge
# RUN make test && make build
RUN make build

# Package binary & certs in a scratch container
FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=intermediate /perf /perf
COPY --from=intermediate /go/src/github.com/Nextdoor/pg-bifrost.git/target/pg-bifrost /
CMD ["/pg-bifrost"]

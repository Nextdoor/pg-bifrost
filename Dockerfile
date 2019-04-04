# Test and build binary
FROM golang:1.11.4-stretch as intermediate
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
ARG is_ci
ENV CI=$is_ci
RUN make test && make build

# Package binary in a scratch container
FROM scratch
COPY --from=intermediate /go/src/github.com/Nextdoor/pg-bifrost.git/target/pg-bifrost /
CMD ["/pg-bifrost"]
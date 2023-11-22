# Copyright AppsCode Inc. and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:{GO_VERSION} AS builder

ARG TARGETOS
ARG TARGETARCH
ARG DB

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends apt-transport-https ca-certificates

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -v -o /wal-g ./main/${DB}/main.go

FROM {ARG_FROM}

ARG TARGETOS
ARG TARGETARCH
ARG DB

LABEL org.opencontainers.image.source https://github.com/kubedb/wal-g

COPY --from=0 /wal-g /wal-g
COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/wal-g"]

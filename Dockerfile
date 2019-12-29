FROM ocaml/opam2:alpine

RUN sudo apk add m4

RUN opam update \
&& opam install -y oml reason ezjsonm lwt_log \
&& opam depext -i tls ssl irmin-unix

ADD src src
RUN sudo chown -R opam:nogroup src
RUN cd src && opam config exec -- dune build --profile release ./main.exe

FROM alpine

RUN adduser nibble --disabled-password

WORKDIR /home/nibble
COPY --from=0 /home/opam/opam-repository/src/_build/default/main.exe ./nibbledb

RUN apk update && apk add gmp libressl zlib openssl

USER nibble

RUN openssl req -x509 -newkey rsa:4096 -keyout /tmp/server.key -out /tmp/server.crt -days 3650 -nodes -subj "/C=UK/ST=foo/L=bar/O=baz/OU= Department/CN=example.com"

EXPOSE 8000



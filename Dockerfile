FROM debian

RUN apt-get update && apt-get install -y build-essential m4 opam

RUN opam init -ya --compiler 4.06.1 \
&& opam update \
&& opam install -y depext oml reason ezjsonm \
&& opam depext -i tls ssl irmin-unix

ADD src src
RUN cd src && opam config exec -- dune build --profile release ./main.exe

FROM debian

RUN useradd -ms /bin/bash nibble

WORKDIR /home/nibble
COPY --from=0 /src/_build/default/main.exe ./nibbledb

RUN apt-get update && apt-get install -y libgmp10 libssl1.1 zlib1g openssl

USER nibble

RUN openssl req -x509 -newkey rsa:4096 -keyout /tmp/server.key -out /tmp/server.crt -days 3650 -nodes -subj "/C=UK/ST=foo/L=bar/O=baz/OU= Department/CN=example.com"

EXPOSE 8000



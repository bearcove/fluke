#!/bin/bash
export SSLKEYLOGFILE=/tmp/ssl
openssl s_client -connect fasterthanli.me:443 -servername fasterthanli.me -alpn h2 -quiet

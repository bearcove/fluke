#!/bin/bash
echo "This accepts connections on port 8000 and proxies"
echo "them to fasterthanli.me, port 443, over TLS, with"
echo "SNI set to fasterthanli.me, and ALPN set to 'h2'."
echo
echo "It is particularly useful to run httpwg-cli against"
echo "real servers. See also ./openssl-connect.sh"
set -eux
socat TCP-LISTEN:8000,reuseaddr,fork EXEC:./openssl-connect.sh

#! /bin/sh
redis-server --port 6379 &
redis-server --tls-port 6378 --port 0 \
	--tls-cert-file ./test/eredis_SUITE_data/certs/redis.crt \
	--tls-key-file ./test/eredis_SUITE_data/certs/redis.key \
	--tls-ca-cert-file ./test/eredis_SUITE_data/certs/ca.crt

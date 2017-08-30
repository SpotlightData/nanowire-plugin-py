install:
	sudo ./setup.py install

test: install
	AMQP_HOST="localhost" \
	AMQP_PORT="5672" \
	AMQP_USER="guest" \
	AMQP_PASS="guest" \
	MINIO_HOST="localhost" \
	MINIO_PORT="9000" \
	MINIO_ACCESS="default" \
	MINIO_SECRET="12345678" \
	MINIO_SCHEME="http" \
	./tester/__init__.py

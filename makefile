VERSION := $(shell cat VERSION)


install:
	sudo ./setup.py install

upload:
	mc mb local/j-00000000-0000-0000-0000-000000000000
	mc cp tester/testfile.txt local/j-00000000-0000-0000-0000-000000000000/t-00000000-0000-0000-0000-000000000000/input/source/testfile.txt

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
	MONITOR_URL="http://localhost:7380" \
	./tester/__init__.py

release:
	./setup.py sdist bdist_wheel
	-twine upload dist/nanowire_plugin-$(VERSION)-py3-none-any.whl
	twine upload dist/nanowire_plugin-$(VERSION).tar.gz

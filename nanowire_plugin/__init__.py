#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""

import logging
from json import loads, dumps, decoder
from os import environ
from os.path import join

import pika
from minio import Minio
from minio.error import AccessDenied


def bind(function: callable, name: str, version="1.0.0"):
    """binds a function to the input message queue"""

    credentials = pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"])
    parameters = pika.ConnectionParameters(
        host=environ["AMQP_HOST"], port=int(environ["AMQP_PORT"]), credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    input_channel = connection.channel()
    output_channel = connection.channel()

    minio_client = Minio(
        environ["MINIO_HOST"] + ":" + environ["MINIO_PORT"],
        access_key=environ["MINIO_ACCESS"],
        secret_key=environ["MINIO_SECRET"],
        secure=True if environ["MINIO_SCHEME"] == "https" else False)
    minio_client.set_app_info(name, version)

    minio_client.list_buckets()

    logging.info("initialised sld lib")

    def send(chan, method, properties, body: str):
        """unwraps a message and calls the user function"""

        logging.debug("consumed message", extra={
            "chan": chan,
            "method": method,
            "properties": properties})

        raw = body.decode("utf-8")

        try:
            payload = loads(raw)
        except decoder.JSONDecodeError as exp:
            logging.error(exp)
            return

        validate_payload(payload, name)

        next_plugin = get_next_plugin(name, payload["nmo"]["job"]["workflow"])
        if next_plugin is None:
            logging.error("next plugin could not be determined", extra={
                "job_id": payload["nmo"]["job"]["job_id"],
                "task_id": payload["nmo"]["task"]["task_id"]})
            return

        path = join(
            payload["nmo"]["task"]["task_id"],
            "input",
            "source",
            payload["nmo"]["source"]["name"])

        if not minio_client.bucket_exists(payload["nmo"]["job"]["job_id"]):
            logging.error("job_id does not have a bucket", extra={
                "job_id": payload["nmo"]["job"]["job_id"],
                "task_id": payload["nmo"]["task"]["task_id"]})
            return

        try:
            url = minio_client.presigned_get_object(payload["nmo"]["job"]["job_id"], path)

        except AccessDenied as exp:
            logging.error(exp, extra={
                "job_id": payload["nmo"]["job"]["job_id"],
                "task_id": payload["nmo"]["task"]["task_id"]})
            return

        # calls the user function to mutate the JSON-LD data

        result = function(payload["nmo"], payload["jsonld"], url)

        if result is None:
            raise TypeError("return value is None")

        if not isinstance(result, dict):
            raise TypeError("return value must be of type dict")

        if "jsonld" in result:
            result = result["jsonld"]
        else:
            result = result

        payload["jsonld"] = result

        output_channel.queue_declare(next_plugin)
        output_channel.basic_publish(
            "",
            next_plugin,
            dumps(payload)
        )

    logging.debug("consuming from", extra={"queue": name})
    input_channel.queue_declare(
        name,
        True,
        True,
        False,
        False,
    )
    input_channel.basic_consume(
        send,
        name,
        no_ack=False,
        exclusive=False,
    )

    try:
        input_channel.start_consuming()
    except pika.exceptions.RecursionError as exp:
        input_channel.stop_consuming()
        connection.close()
        raise exp


def validate_payload(payload: dict, name: str):
    if "job" not in payload["nmo"]:
        logging.error("no job in nmo")
        return

    if "task" not in payload["nmo"]:
        logging.error("no task in nmo")
        return

    if not ensure_this_plugin(name, payload["nmo"]["job"]["workflow"]):
        logging.error("declared plugin name does not match workflow", extra={
            "job_id": payload["nmo"]["job"]["job_id"],
            "task_id": payload["nmo"]["task"]["task_id"]})
        return


def ensure_this_plugin(this_plugin: str, workflow: list)->bool:
    for workpipe in workflow:
        if workpipe["config"]["name"] == this_plugin:
            return True
    return False


def get_next_plugin(this_plugin: str, workflow: list) -> str:
    found = False
    for workpipe in workflow:
        if not found:
            if workpipe["config"]["name"] == this_plugin:
                found = True
        else:
            return workpipe["config"]["name"]

    return None

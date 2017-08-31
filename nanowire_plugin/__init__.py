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

        payload = body.decode("utf-8")

        try:
            obj = loads(payload)
        except decoder.JSONDecodeError as exp:
            logging.error(exp)
            return

        if "jsonld" not in obj:  # first plugin, downloads file
            if "job" not in obj:
                logging.error("no job in nmo")
                return

            if "task" not in obj:
                logging.error("no task in nmo")
                return

            job_id = obj["job"]["job_id"]
            task_id = obj["task"]["task_id"]
            next_plugin = get_next_plugin(name, obj["job"]["workflow"])
            if next_plugin is None:
                logging.error("next plugin could not be determined", extra={
                    "job_id": job_id, "task_id": task_id})
                return

            if not ensure_this_plugin(name, obj["job"]["workflow"]):
                logging.error("declared plugin name does not match workflow", extra={
                    "job_id": job_id, "task_id": task_id})
                return

            path = join(task_id, "input", "source", obj["sources"][0]["name"])
            result = None

            if not minio_client.bucket_exists(job_id):
                logging.error("job_id does not have a bucket", extra={
                    "job_id": job_id, "task_id": task_id})
                return

            try:
                response = minio_client.get_object(job_id, path)
                initial = response.data

                result = function(initial)

            except AccessDenied as exp:
                logging.error(exp, extra={
                    "job_id": job_id, "task_id": task_id})
                return

            if result is not None:
                if not isinstance(result, dict):
                    raise TypeError("return value must be of type dict")

                if "jsonld" in result:
                    result = result["jsonld"]
                else:
                    result = result

                result = dumps({
                    "nmo": obj,
                    "jsonld": result
                })

        else:  # other plugins, operates on json-ld
            next_plugin = get_next_plugin(name, obj["nmo"]["job"]["workflow"])
            if next_plugin is None:
                logging.error("next plugin could not be determined", extra={
                    "job_id": job_id, "task_id": task_id})
                return

            if not ensure_this_plugin(name, obj["job"]["workflow"]):
                logging.error("declared plugin name does not match workflow", extra={
                    "job_id": job_id, "task_id": task_id})
                return

            result = {
                "nmo": obj["nmo"],
                "jsonld": function(body)
            }

        if result is not None:
            output_channel.queue_declare(next_plugin)
            output_channel.basic_publish(
                "",
                next_plugin,
                result
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
        no_ack=True,
        exclusive=False,
    )

    try:
        input_channel.start_consuming()
    except pika.exceptions.RecursionError as exp:
        input_channel.stop_consuming()
        connection.close()
        raise exp


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

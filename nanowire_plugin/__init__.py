#!/usr/bin/env python3
"""
Provides a `bind` function to plugins so they can simply bind a function to a queue.
"""

import logging
from json import loads, dumps, decoder
from os import environ
from os.path import join
import urllib
import time

import pika
from pythonjsonlogger import jsonlogger
from minio import Minio
from minio.error import AccessDenied


LOG = logging.getLogger()
HND = logging.StreamHandler()
HND.setFormatter(jsonlogger.JsonFormatter())
LOG.addHandler(HND)


class ProcessingError(Exception):
    """Exception raised for errors during processing.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str, job_id: str=None, task_id: str=None, extra: dict=None):
        super(ProcessingError, self).__init__(message)
        self.message = message
        self.extra = extra or {}
        self.meta = {"job_id": job_id, "task_id": task_id}


def bind(function: callable, name: str, version="1.0.0"):
    """binds a function to the input message queue"""

    parameters = pika.ConnectionParameters(
        host=environ["AMQP_HOST"],
        port=int(environ["AMQP_PORT"]),
        credentials=pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"]))

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

    monitor_url = environ["MONITOR_URL"]

    logging.info("initialised sld lib")

    def send(chan, method, properties, body: str):
        """unwraps a message and calls the user function"""

        logging.info("consumed message", extra={
            "chan": chan,
            "method": method,
            "properties": properties})

        raw = body.decode("utf-8")
        payload = loads(raw)
        validate_payload(payload, name)

        next_plugin = get_next_plugin(name, payload["nmo"]["job"]["workflow"])
        if next_plugin is None:
            logging.info("this is the final plugin", extra={
                "job_id": payload["nmo"]["job"]["job_id"],
                "task_id": payload["nmo"]["task"]["task_id"]})

        path = join(
            payload["nmo"]["task"]["task_id"],
            "input",
            "source",
            payload["nmo"]["source"]["name"])

        if not minio_client.bucket_exists(payload["nmo"]["job"]["job_id"]):
            raise ProcessingError(
                "job_id does not have a bucket",
                job_id=payload["nmo"]["job"]["job_id"],
                task_id=payload["nmo"]["task"]["task_id"])

        url = minio_client.presigned_get_object(payload["nmo"]["job"]["job_id"], path)

        # calls the user function to mutate the JSON-LD data

        result = function(payload["nmo"], payload["jsonld"], url)

        # if there are issues, just use the input and carry on the pipeline

        if result is None:
            logging.error("return value is None")
            result = payload["jsonld"]

        if not isinstance(result, dict):
            logging.error("return value must be of type dict, not %s", type(result))
            result = payload["jsonld"]

        if "jsonld" in result:
            result = result["jsonld"]
        else:
            result = result

        payload["jsonld"] = result

        logging.info("finished running user code", extra={
            "job_id": payload["nmo"]["job"]["job_id"],
            "task_id": payload["nmo"]["task"]["task_id"]})

        input_channel.basic_ack(method.delivery_tag)

        if next_plugin:
            output_channel.queue_declare(
                next_plugin,
                False,
                True,
                False,
                False,
            )
            output_channel.basic_publish(
                "",
                next_plugin,
                dumps(payload)
            )

        return {"job_id": payload["nmo"]["job"]["job_id"], "task_id": payload["nmo"]["task"]["task_id"]}

    logging.info("consuming from", extra={"queue": name})

    try:
        while True:
            input_channel.queue_declare(name, False, True)
            method_frame, header_frame, body = input_channel.basic_get(name)
            if (method_frame, header_frame, body) == (None, None, None):
                continue  # queue empty

            if body is None:
                logging.error("body received was empty")
                continue  # body empty

            meta = {}

            try:
                meta = send(input_channel, method_frame, header_frame, body)

            except ProcessingError as exp:
                input_channel.basic_reject(method_frame.delivery_tag, False)
                logging.error("Processing Error: " + exp.message, extra={**exp.meta, **exp.extra})

                urllib.request.Request(
                    join(monitor_url, "/v2/task/status/",
                         exp.meta["job_id"], exp.meta["task_id"]),
                    data=dumps({
                        "t": int(time.time()),
                        "p": name,
                        "e": exp.message
                    }).encode(),
                    headers={"Content-Type: application/json"})

            except Exception as exp:
                input_channel.basic_reject(method_frame.delivery_tag, False)
                logging.error("Other Error: " + exp)

            urllib.request.Request(
                join(monitor_url, "/v2/task/status/",
                     meta["job_id"], meta["task_id"]),
                data=dumps({
                    "t": int(time.time()),
                    "p": name
                }).encode(),
                headers={"Content-Type: application/json"})

    except pika.exceptions.RecursionError as exp:
        input_channel.stop_consuming()
        connection.close()
        raise exp


def validate_payload(payload: dict, name: str) -> bool:
    """ensures payload includes the required metadata and this plugin is in there"""

    if "nmo" not in payload:
        raise ProcessingError("no job in nmo")

    if "job" not in payload["nmo"]:
        raise ProcessingError("no job in nmo")

    if "task" not in payload["nmo"]:
        raise ProcessingError("no task in nmo")

    if not ensure_this_plugin(name, payload["nmo"]["job"]["workflow"]):
        raise ProcessingError(
            "declared plugin name does not match workflow",
            job_id=payload["nmo"]["job"]["job_id"],
            task_id=payload["nmo"]["task"]["task_id"])


def ensure_this_plugin(this_plugin: str, workflow: list)->bool:
    """ensures the current plugin is present in the workflow"""
    for workpipe in workflow:
        if workpipe["config"]["name"] == this_plugin:
            return True
    return False


def get_next_plugin(this_plugin: str, workflow: list) -> str:
    """returns the next plugin in the sequence"""
    found = False
    for workpipe in workflow:
        if not found:
            if workpipe["config"]["name"] == this_plugin:
                found = True
        else:
            return workpipe["config"]["name"]

    return None

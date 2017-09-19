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
from minio import Minio
from minio.error import AccessDenied


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

    logger = logging.getLogger("nanowire-plugin")
    logger.info("initialising nanowire lib")

    parameters = pika.ConnectionParameters(
        host=environ["AMQP_HOST"],
        port=int(environ["AMQP_PORT"]),
        credentials=pika.PlainCredentials(environ["AMQP_USER"], environ["AMQP_PASS"]),
        heartbeat_interval=600)

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

    logger.setLevel(logging.DEBUG)
    logger.info("initialised nanowire lib", extra={
        "monitor_url": monitor_url,
        "minio": environ["MINIO_HOST"],
        "rabbit": environ["AMQP_HOST"]
    })

    sys_env = [
        "AMQP_HOST",
        "AMQP_PORT",
        "AMQP_USER",
        "AMQP_PASS",
        "MINIO_HOST",
        "MINIO_PORT",
        "MINIO_ACCESS",
        "MINIO_SECRET",
        "MINIO_SCHEME",
        "MONITOR_URL"
    ]

    def send(method, payload: dict):
        """unwraps a message and calls the user function"""

        this = get_this_plugin(name, payload["nmo"]["job"]["workflow"])
        if this == -1:
            raise ProcessingError(
                "declared plugin name does not match workflow",
                job_id=payload["nmo"]["job"]["job_id"],
                task_id=payload["nmo"]["task"]["task_id"])

        try:
            set_status(monitor_url,
                       payload["nmo"]["job"]["job_id"],
                       payload["nmo"]["task"]["task_id"],
                       name + ".consumed", error)
        except Exception as exp:
            logger.warning("failed to set status", extra={
                "exception": str(exp),
                "job_id": payload["nmo"]["job"]["job_id"],
                "task_id": payload["nmo"]["task"]["task_id"]})

        next_plugin = get_next_plugin(name, payload["nmo"]["job"]["workflow"])
        if next_plugin is None:
            logger.info("this is the final plugin", extra={
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

        if "env" in payload["nmo"]["job"]["workflow"][this]:
            if isinstance(payload["nmo"]["job"]["workflow"][this]["env"], dict):
                for ename, evalue in payload["nmo"]["job"]["workflow"][this]["env"].items():
                    if ename in sys_env:
                        logger.error("attempt to set plugin env var", extra={
                            "name": ename,
                            "attempted_value": evalue})
                        continue

                    environ[ename] = evalue

        result = function(payload["nmo"], payload["jsonld"], url)

        # if there are issues, just use the input and carry on the pipeline

        if result is None:
            logger.error("return value is None")
            result = payload["jsonld"]

        if not isinstance(result, dict):
            logger.error("return value must be of type dict, not %s", type(result))
            result = payload["jsonld"]

        if "jsonld" in result:
            result = result["jsonld"]
        else:
            result = result

        payload["jsonld"] = result

        logger.info("finished running user code", extra={
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

        return {
            "job_id": payload["nmo"]["job"]["job_id"],
            "task_id": payload["nmo"]["task"]["task_id"]
        }

    logger.info("consuming from", extra={"queue": name})

    try:
        while True:
            queue_state = input_channel.queue_declare(name, False, True, False, False)
            if queue_state.method.message_count == 0:
                time.sleep(3)
                continue

            method_frame, header_frame, body = input_channel.basic_get(name)
            if (method_frame, header_frame, body) == (None, None, None):
                time.sleep(3)
                continue  # queue empty

            if body is None:
                logger.error("body received was empty")
                time.sleep(3)
                continue  # body empty

            meta = {"job_id": None, "task_id": None}
            error = ""

            try:
                raw = body.decode("utf-8")
                payload = loads(raw)
                validate_payload(payload)
            except Exception as exp:
                logger.exception(exp)
                continue

            logger.info("consumed message", extra={
                "job_id": meta["job_id"],
                "task_id": meta["task_id"]})

            try:
                send(method_frame, payload)

            except ProcessingError as exp:
                input_channel.basic_reject(method_frame.delivery_tag, False)
                logger.exception("Processing Error: " + exp.message,
                                 extra={**exp.meta, **exp.extra})

                error = exp.message
                meta = exp.meta

            except Exception as exp:
                input_channel.basic_reject(method_frame.delivery_tag, False)
                logger.exception(exp)

            finally:
                if meta["job_id"] is not None and meta["task_id"] is not None:
                    try:
                        set_status(monitor_url, meta["job_id"],
                                   meta["task_id"], name + ".done", error)
                    except Exception as exp:
                        logger.warning("failed to set status", extra={
                            "exception": str(exp),
                            "job_id": meta["job_id"],
                            "task_id": meta["task_id"],
                            "error": error})

    except pika.exceptions.RecursionError as exp:
        connection.close()
        raise exp


def validate_payload(payload: dict) -> bool:
    """ensures payload includes the required metadata and this plugin is in there"""

    if "nmo" not in payload:
        raise ProcessingError("no job in nmo")

    if "job" not in payload["nmo"]:
        raise ProcessingError("no job in nmo")

    if "task" not in payload["nmo"]:
        raise ProcessingError("no task in nmo")


def get_this_plugin(this_plugin: str, workflow: list)->int:
    """ensures the current plugin is present in the workflow"""
    for i, workpipe in enumerate(workflow):
        if workpipe["config"]["name"] == this_plugin:
            return i
    return -1


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


def set_status(monitor_url: str, job_id: str, task_id: str, name: str, error: str):
    """sends a POST request to the monitor to notify it of task position"""
    data = dumps({
        "t": int(time.time() * 1000),
        "id": task_id,
        "p": name,
        "e": error
    }).encode()

    if "TESTING_MODE" in environ:
        logging.info(data)
        return

    req = urllib.request.Request(
        urllib.parse.urljoin(
            monitor_url,
            "/v3/task/status/%s/%s" % (job_id, task_id)),
        data=data,
        headers={
            "Content-Type": "application/json"
        })
    urllib.request.urlopen(req)

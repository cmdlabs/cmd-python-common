"""
runCMD library to support AWS Neptune
"""
import time
import requests
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

def check_response_code(resp):
    """
    Checks the response code of a response and errors if it doesn't work.
    """
    if resp.status_code != 200:
        LOG.error("Code=%s, reason=%s, text=%s" % (resp.status_code, resp.reason, resp.text))
        raise Exception("Invalid status code received from Neptune.")

def wait_until_loaded(neptune_endpoint, loader_id, port):
    """
    Performs a load operation and waits until the load is finished.
    """
    finished = False
    while not finished:
        response = neptune_loader_status(neptune_endpoint, port, loader_id)
        check_response_code(response)
        status = response.json()["payload"]["overallStatus"]["status"]
        LOG.info("Status=%s" % status)
        if status == "LOAD_IN_PROGRESS":
            time.sleep(10)
        elif status == "LOAD_COMPLETED":
            finished = True
        else:
            raise Exception("Load did not complete successfully, status=%s" % status)

def load_to_neptune(s3_input, neptune_endpoint, region, iam_role, port=8182):
    """
    Loads data into Neptune
    """
    payload = {}
    payload["source"] = s3_input
    payload["format"] = "csv"
    payload["failOnError"] = "TRUE"
    payload["region"] = region
    payload["iamRoleArn"] = iam_role
    LOG.info("payload=%s" % payload)
    neptune_url = "http://%s:%s/loader" % (neptune_endpoint, port)
    LOG.info("Neptune URL=%s", neptune_url)
    response = requests.post(neptune_url, json=payload)
    LOG.info("Code=%s, reason=%s, text=%s" % (response.status_code, response.reason, response.text))
    check_response_code(response)
    loader_id = response.json()["payload"]["loadId"]
    wait_until_loaded(neptune_endpoint, port, loader_id)

def neptune_status(neptune_endpoint, port=8182):
    """
    Find the status of the Neptune Server
    """
    neptune_url = "http://%s:%s/status" % (neptune_endpoint, port)
    LOG.info("Neptune URL=%s" % neptune_url)
    response = requests.get(neptune_url)
    LOG.info("Code=%s, reason=%s, text=%s" % (response.status_code, response.reason, response.text))

def neptune_loader_status(neptune_endpoint, loader_id, port=8182):
    """
    Find the status of a Neptune Load.
    """
    neptune_url = "http://%s:%s/loader?loadId=%s" % (neptune_endpoint, port, loader_id)
    LOG.info("Neptune URL=%s", neptune_url)
    response = requests.get(neptune_url)
    LOG.info("Code=%s, reason=%s, text=%s" % (response.status_code, response.reason, response.text))
    return response

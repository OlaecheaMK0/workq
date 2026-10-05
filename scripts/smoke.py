#!/usr/bin/env python3
"""Check the running local API with synthetic jobs, using only the stdlib."""
import json
import os
import time
import urllib.error
import urllib.request
import uuid

BASE = os.getenv("WORKQ_URL", "http://127.0.0.1:18380").rstrip("/")
TOKEN = os.getenv("WORKQ_API_TOKEN", "")


def request(method, path, payload=None, key=None):
    headers = {"Content-Type": "application/json"}
    if TOKEN:
        headers["Authorization"] = "Bearer " + TOKEN
    if key:
        headers["Idempotency-Key"] = key
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def main():
    status, _ = request("GET", "/readyz")
    assert status == 200, "API is not ready"
    prefix = "smoke-" + str(uuid.uuid4())
    jobs = []
    for label, failures in [("success", 0), ("retry", 2), ("dead", 10)]:
        payload = {"kind": "report", "payload": {"name": prefix + label, "items": [10, 20, 30], "fail_until_attempt": failures}, "max_attempts": 3}
        key = prefix + label
        status, data = request("POST", "/v1/jobs", payload, key)
        assert status == 201, data
        job_id = data["job"]["id"]
        status, duplicate = request("POST", "/v1/jobs", payload, key)
        assert status == 200 and duplicate["job"]["id"] == job_id, duplicate
        payload["payload"]["items"] = [100]
        status, _ = request("POST", "/v1/jobs", payload, key)
        assert status == 409, "changed payload should conflict"
        jobs.append((label, job_id))
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        states = [(label, request("GET", "/v1/jobs/" + job_id)[1]) for label, job_id in jobs]
        if all(job.get("state") in ("succeeded", "dead") for _, job in states):
            for label, job in states:
                expected = "dead" if label == "dead" else "succeeded"
                assert job["state"] == expected, job
                assert job["attempts"] == (1 if label == "success" else 3), job
                if label != "dead":
                    assert job["result"]["sum"] == 60, job
                print(label + ": " + job["state"] + " after " + str(job["attempts"]) + " attempt(s)")
            print("HTTP smoke passed: submission identity, conflicts, success, retries, dead letters")
            return
        time.sleep(0.2)
    raise AssertionError("Jobs did not reach terminal state before timeout")


if __name__ == "__main__":
    main()

import requests
import time
import random

def sync_commission(data, max_retries=3):
    url = "http://example.com/sync-commission"
    random.seed(url)
    backoff = 1
    for attempt in range(max_retries):
        try:
            # Simulate possible failures
            n = random.random()
            if n < 0.2:
                raise requests.exceptions.Timeout("Simulated timeout")
            elif n < 0.35:
                # Simulate HTTP 429/500
                class Resp: status_code=429
                resp = Resp()
            else:
                class Resp: status_code=200
                resp = Resp()

            if resp.status_code == 200:
                # print("API sync success!")
                return True
            if resp.status_code in (429, 500):
                raise requests.exceptions.RequestException("Server error")

        except Exception:
            # print("Backing off and retrying...")
            if attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= 2
            else:
                # print("Max retries exceeded. Aborting...")
                return False
    return False

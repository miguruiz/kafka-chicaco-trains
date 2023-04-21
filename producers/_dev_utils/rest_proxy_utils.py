import json
import requests


REST_PROXY_URL = "http://localhost:8082"

def get_topics(REST_PROXY_URL):
    """Gets topics from REST Proxy"""
    # See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics

    resp = requests.get(f"{REST_PROXY_URL}/topics")

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))
    return resp.json()


def get_topic(REST_PROXY_URL, topic_name):
    """Get specific details on a topic"""
    # See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def produce_msg(REST_PROXY_URL, topic_name):
    resp = requests.post(
        f"http://localhost:8082/topics/chicago.rail.weather",
        headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
        data=json.dumps(
            {"value_schema": json.dumps({
          "namespace": "com.udacity",
          "type": "record",
          "name": "weather.value",
          "fields": [
            {
              "name": "temperature",
              "type": "float"
            },
            {
              "name": "status",
              "type": "string"
            }
          ]
        }),
             "key_schema": json.dumps({'fields': [{'name': 'timestamp', 'type': 'long'}], 'name': 'weather.key',
                            'namespace': 'com.udacity', 'type': 'record'}),
             "records": [
                 {"value":
                     {
                         "temperature": 70.0,
                         "status": "sunny"
                     }
                 }
             ]}
        ),
    )
    return resp

if __name__ == "__main__":
    resp = produce_msg(REST_PROXY_URL,"chicago.rail.weather")
    print(resp.json())




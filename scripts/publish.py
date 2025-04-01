import argparse

from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient

from config import settings

settings.load_env()


def publish(project_id, topic_id, msg):
    publisher = PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = msg.encode("utf-8")
    try:
        future = publisher.publish(topic_path, data)
        return future.result()
    except NotFound:
        publisher.create_topic(request={"name": topic_path})
        future = publisher.publish(topic_path, data)
        return future.result()


def main(args):
    print(publish(args.project, args.topic, args.message))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("topic")
    parser.add_argument("message")
    parser.add_argument("--project", default="local-dev")
    args = parser.parse_args()

    main(args)

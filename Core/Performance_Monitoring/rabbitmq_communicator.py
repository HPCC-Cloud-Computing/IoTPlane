from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
import json
import socket


def request_service(conn, message_request, exchange_request, request_routing_key):
    id_response = uuid()
    queue_response = Queue(name=id_response, exchange=exchange_request, routing_key=id_response, exclusive=True, auto_delete=True)
    message_request['reply_to'] = id_response
    conn.ensure_connection()
    with Producer(conn) as producer:
        producer.publish(
            json.dumps(message_request),
            exchange=exchange_request.name,
            routing_key=request_routing_key,
            declare=[queue_response],
            retry=True
        )

    message_response = None

    def on_response(body, message):
        nonlocal message_response
        message_response = json.loads(body)
    try:

        with Consumer(conn, queues=queue_response, callbacks=[on_response], no_ack=True):
            try:
                while message_response is None:
                    conn.drain_events(timeout=10)
            except socket.timeout:
                return {
                    'error': 'Can not connect to service'
                }
    except Exception:
        print("cannot create Consumer: " + request_routing_key)
        return {
            'error': 'Cannot create Consumer'
        }

    return message_response


def publish_message(producer_connection, exchange, message, routing_key):
    producer_connection.ensure_connection()
    with Producer(producer_connection) as producer:
        producer.publish(
            json.dumps(message),
            exchange=exchange.name,
            routing_key=routing_key,
            retry=True
        )

import json
import os
import pika


def get_rabbitmq_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


ROUTES = {
    'physical': 'orders.physical',
    'digital': 'orders.digital',
    'subscription': 'orders.subscription',
}

UNROUTABLE_QUEUE = 'orders.unroutable'


def route_order(ch, method, properties, body):
    try:
        order = json.loads(body)
        order_id = order['orderId']
        items = order['items']
        if not isinstance(items, list):
            raise TypeError("items is not a list")
    except (ValueError, KeyError, TypeError) as e:
        print(f"[Router] Dropping malformed order: {e!r} body={body[:200]!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"[Router] Processing order {order_id}")

    connection = get_rabbitmq_connection()
    channel = connection.channel()

    for queue in list(ROUTES.values()) + [UNROUTABLE_QUEUE]:
        channel.queue_declare(queue=queue, durable=True)

    item_count = len(items)
    props = pika.BasicProperties(delivery_mode=2, content_type='application/json')

    for index, item in enumerate(items):
        message = {
            'orderId': order_id,
            'correlationId': order_id,
            'itemIndex': index,
            'totalItems': item_count,
            'item': item,
        }
        item_type = item.get('type') if isinstance(item, dict) else None
        queue = ROUTES.get(item_type)
        if queue is None:
            print(f"[Router] Unknown type {item_type!r} for {order_id}/{index}; sending to {UNROUTABLE_QUEUE}")
            queue = UNROUTABLE_QUEUE
        channel.basic_publish(exchange='', routing_key=queue,
                              body=json.dumps(message), properties=props)

    connection.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Router] Order {order_id} split into {item_count} items and routed")


def main():
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.incoming', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='orders.incoming', on_message_callback=route_order)
    print('[Router] Waiting for orders...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
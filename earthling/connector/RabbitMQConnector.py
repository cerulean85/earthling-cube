import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import pika, yaml

class RabbitMQConnector:
    _instance = None
    address = ''
    port = ''
    username = ''
    password = ''

    def set_compose(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = root_dir.split('/')
        if root_dir[len(root_dir) - 1] == 'handler':
            tmp = ''
            for i in range(0, len(root_dir) - 1):
                dir_str = root_dir[i]
                tmp = tmp + dir_str + '/'

        with open(f'earth-compose.yml') as f:
            compose = yaml.load(f, Loader=yaml.FullLoader)
            compose = compose['mq']
            self.address = compose['address']
            self.port = compose['port']
            self.username = compose['username']
            self.password = compose['password']

    def connect_rabbit_mq(self):

        # username = 'rabbitmq'
        # password = 'rabbitmq'
        # host = '211.195.9.228'
        self.set_compose()
        credentials = pika.PlainCredentials(username=self.username, password=self.password)

        # https://velog.io/@owlur/pika%EC%97%90%EC%84%9C-TCP-keepalive-%EC%84%A4%EC%A0%95%ED%95%98%EA%B8%B0
        # hearbeat, timeout 관련...
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.address, credentials=credentials, heartbeat=0, tcp_options={'TCP_KEEPIDLE':60}))
        # connection = pika.BlockingConnection(pika.ConnectionParameters(host="211.195.9.228"))
        # connection = pika.BlockingConnection(pika.URLParameters('amqp://rabbitmq:rabbitmq@211.195.9.228:5672'))
        return connection


    def __init__(self):
        if not RabbitMQConnector._instance:
            self.connection = self.connect_rabbit_mq()


    @classmethod
    def getInstance(cls):
        if not cls._instance:
            cls._instance = RabbitMQConnector()
        return cls._instance


    def getConnection(self):
        return self.connection


task_queue_name = 'task'
result_queue_name = 'result'

def task_basic_pubilsh(user_id, message):
    connection = RabbitMQConnector().getInstance().getConnection()
    channel = connection.channel()
    queue_name = f"{task_queue_name}{user_id}"
    channel.queue_declare(queue=queue_name, durable=False)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    # print(f"queue_name: {queue_name}, message: {message} ")
    channel.close()
    return queue_name



class AsyncRabbitMQConnector:
    # _instance = None
    address = ''
    port = ''
    username = ''
    password = ''

    def set_compose(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = root_dir.split('/')
        if root_dir[len(root_dir) - 1] == 'handler':
            tmp = ''
            for i in range(0, len(root_dir) - 1):
                dir_str = root_dir[i]
                tmp = tmp + dir_str + '/'

        with open(f'earth-compose.yml') as f:
            compose = yaml.load(f, Loader=yaml.FullLoader)
            compose = compose['mq']
            self.address = compose['address']
            self.port = compose['port']
            self.username = compose['username']
            self.password = compose['password']

    def connect_rabbit_mq(self):

        # username = 'rabbitmq'
        # password = 'rabbitmq'
        # host = '211.195.9.228'
        self.set_compose()
        credentials = pika.PlainCredentials(username=self.username, password=self.password)

        # https://velog.io/@owlur/pika%EC%97%90%EC%84%9C-TCP-keepalive-%EC%84%A4%EC%A0%95%ED%95%98%EA%B8%B0
        # hearbeat, timeout 관련...
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.address, credentials=credentials, heartbeat=0, tcp_options={'TCP_KEEPIDLE':60}))
        # connection = pika.BlockingConnection(pika.ConnectionParameters(host="211.195.9.228"))
        # connection = pika.BlockingConnection(pika.URLParameters('amqp://rabbitmq:rabbitmq@211.195.9.228:5672'))
        return connection


    # def __init__(self):
    #     if not RabbitMQConnector._instance:
    #         self.connection = self.connect_rabbit_mq()


    # @classmethod
    # def getInstance(cls):
    #     if not cls._instance:
    #         cls._instance = RabbitMQConnector()
    #     return cls._instance


    def getConnection(self):
        return self.connection

def task_basic_pubilsh_async(user_id, message):
    connection = AsyncRabbitMQConnector().connect_rabbit_mq()
    channel = connection.channel()
    queue_name = f"{task_queue_name}{user_id}"
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    # print(f"queue_name: {queue_name}, message: {message} ")
    channel.close()
    connection.close()
    return queue_name

import time, pickle, json
import earthling_edum.proto as proto
# import handler
# from handler.BasicDAO import *
from multiprocessing import Process, Value, Queue
from .Com import Com
from .Logging import log
from .Monitor import Monitor
# from handler.RabbitMQConnector import RabbitMQConnector, task_basic_pubilsh
from earthling.handler.KafkaHandler import *
from earthling.handler.BasicDAO import *

def consume_action(message):
    try:
        return json.loads(message.decode('utf-8'))
    except:
        return message.decode('utf-8')


def consume():

    consumer = get_consumer("result", consume_action)
    for message in consumer:
        try:
            message = message.value
            task_no = message["task_no"]
            channel = message["channel"]
            user_id = message["user_id"]
            print(f"({user_id})의 task-[{task_no}]를 처리 중입니다.")

            filePath = f"/data/model/connect/{user_id}.pickle"
            with open(filePath,'wb') as fw:
                pickle.dump(message, fw)

            update_state_to_finish(task_no)
            print(f"({user_id})의 task-[{task_no}]를 완료하였습니다. !!")

        except Exception as err:
            print(err)
            print("Passed")


    # RabbitMQ...
    # connection = RabbitMQConnector.getInstance().getConnection()
    # channel = connection.channel()
    # queue_name = f"task"
    # channel.queue_declare(queue=queue_name, durable=False)
    # channel.basic_consume(on_message_callback=callback, queue=queue_name, auto_ack=False)
    # try:
    #     print("Waiting for messages.")
    #     channel.start_consuming()
    # except KeyboardInterrupt:
    #     print('Ctrl+C is Pressed.')


def produce_action(message):
    try:
        return json.dumps(message, ensure_ascii=False)
    except:
        return message.decode('utf-8')
    

def produce(message):
    producer = get_producer(produce_action)
    producer.send("result", value=message)
    producer.flush()
#     task_basic_pubilsh('', 'qwqwqwzzz')


def run():
    print("start!")

    consume()
#     compose = Monitor().get_compose()

#     mng_addr, mng_port   =  compose['manager']['address'], compose['manager']['port']
#     host_addr, host_port =  compose['host']['address'], compose['host']['port']
    
#     task_queue = Queue() # Shared Queue

#     WorkerPool.getInstance(action).set_task_queue(task_queue)  # Shared Queue 설정

#     shared_idle_count = Value('i', 0) # Shared Variable
#     earthling = proto.AssistantEarthling(shared_idle_count) # Shared Variable 설정
#     decorator = proto.AssistantEarthlingDecorator(earthling) # Decorate Target 객체 설정
#     ass = ComAssistant(decorator) # Decorator 객체 설정

#     # Manager Server 연결 확인
#     message = str(host_port) if 'int' in str(type(host_port)) else host_port
#     try:
#         echoed = ass.decorator.echo(mng_addr, mng_port, message)
#         print(f"Manager로부터 받은 echo 메시지: {echoed}")
#     except:
#         print("Manager 서버에 연결할 수 없습니다.")

#     p = Process(target=ass.loop, args=())
#     p.start()

#     print(f"Started Assistant Server From {host_addr}:{host_port}")
#     ass.serve()


# if __name__ == '__main__':
#     run(action)
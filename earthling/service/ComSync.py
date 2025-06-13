import pickle, json
from connector.kafka_modules import get_consumer, get_producer
from handler.BaseDAO import BaseDAO

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
            site = message["site"]
            user_id = message["user_id"]
            print(f"({user_id})의 task-[{task_no}]를 처리 중입니다.")

            filePath = f"/data/model/connect/{user_id}.pickle"
            with open(filePath,'wb') as fw:
                pickle.dump(message, fw)

            dao = BaseDAO()
            dao.update_state_to_finish(task_no)
            print(f"({user_id})의 task-[{task_no}]를 완료하였습니다. !!")

        except Exception as err:
            print(err)
            print("Passed")

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


# from ..handler.EarthlingPoolConnector import exec as exec_earth
from earthling_edum.handler.EduminingPoolConnector import exec as exec_edum
import time

'''

테스트 시나리오

1) 10초 후에 anal_type = 2인 10개의 overview 데이터의 delayed = 'Y'로 업데이트.
manager는 이 task를 제대로 감지하는가?

'''


def test_scene():
    scenes = [
        # scene_1,
        # scene_2
        scene_1_1
    ]

    for scene in scenes:
        scene()

def scene_1():

    ## 페이징... https://inpa.tistory.com/entry/MYSQL-%F0%9F%93%9A-LIMIT-OFFSET
    query = 'SELECT count(*) AS cnt FROM edu_data_artifact WHERE anal_type = 2'
    result = exec_edum(query)
    total_count = result[0]['cnt']
    unit_count = 10
    repeat_count = int(total_count / unit_count)
    offset = 0
    for i in range(0, repeat_count):
        offset = unit_count * i
        query = f'SELECT no FROM edu_data_artifact WHERE anal_type = 2 ORDER BY no DESC LIMIT {unit_count} OFFSET {offset}'
        result = exec_edum(query)
        for row in result:
            no = row['no']
            query = f"UPDATE edu_data_artifact SET `delayed` = 'Y' WHERE no = {no}"
            exec_edum(query)
        time.sleep(10)

    time.sleep(10)
    if total_count > unit_count * repeat_count:
        offset = unit_count * repeat_count
        query = f'SELECT no FROM edu_data_artifact WHERE anal_type = 2 ORDER BY no DESC LIMIT {unit_count} OFFSET {offset}'
        result = exec_edum(query)
        for row in result:
            no = row['no']
            query = f"UPDATE edu_data_artifact SET `delayed` = 'Y' WHERE no = {no}"
            exec_edum(query)

    # query = 'SELECT no FROM edu_data_artifact WHERE anal_type = 2 ORDER BY no DESC LIMIT 10'
    # result = exec_edum(query)
    # for row in result:
    #     no = row['no']
    #     query = f"UPDATE edu_data_artifact SET `delayed` = 'Y' WHERE no = {no}"
    #     exec_edum(query)

def scene_1_1():
    limit_count = 1
    query = f'SELECT no FROM edu_data_artifact WHERE anal_type = 2 ORDER BY no DESC LIMIT {limit_count}'
    result = exec_edum(query)
    for row in result:
        no = row['no']
        query = f"UPDATE edu_data_artifact SET `delayed` = 'Y' WHERE no = {no}"
        exec_edum(query)


'''
2) 10초 후에 anal_type = 2인 10개의 overview 데이터의 delayed = 'Y'로 업데이트 되었을 때,
manager는 assistant에게 요청하여 유휴한 worker를 알아낼 수 있는가?
'''

def scene_2():


    '''
    scene_1()을 실행하여 state를 변경하였다면, 
    idle_count를 3개 가져올 수 있는가?
    
    '''
    pass

'''

3) 10초 후에 analy_type = 2인 10개의 overview 데이터의 delayed = 'Y'로 업데이트 되었을 때,
manager는 assistant에게 요청하여 유휴한 worker에게 task를 분배하는가?



10초 후에 anal_type = 2의 overview 10개가 업데이트


10초 후에 anal_type = 2의 overview 10개가 업데이트


10초 후에 anal_type = 2의 overview 10개가 업데이트


10초 후에 anal_type = 2의 overview 10개가 업데이트



'''
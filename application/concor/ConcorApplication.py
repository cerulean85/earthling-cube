from collections import defaultdict
from io import StringIO
from itertools import combinations
import json, sys, os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import QueryPipeTaskClean, QueryPipeTaskConcor
from earthling.connector.s3_module import read_file_from_s3
from sklearn.feature_extraction.text import TfidfVectorizer

class ConcorApplication:
    def execute(self, task):
        prev_task_id = task["prev_task_id"]
        query = QueryPipeTaskClean()
        searched = query.get_task_by_id(prev_task_id)

        query = QueryPipeTaskConcor()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)

        resource_origin = read_file_from_s3(searched.s3_url)
        data = json.loads(resource_origin)

        co_matrix = defaultdict(lambda: defaultdict(int))
        vocab = set()

        # 각 문장에서 단어들 추출 및 조합
        for item in data:
            words = [token['word'] for token in item['tokens']]
            vocab.update(words)
            # 중복 제거 후 단어쌍 생성 (동일 문장 내 동시 출현)
            for w1, w2 in combinations(set(words), 2):
                co_matrix[w1][w2] += 1
                co_matrix[w2][w1] += 1  # 대칭 행렬

        vocab = sorted(vocab)
        df = pd.DataFrame(index=vocab, columns=vocab).fillna(0)
        for w1 in vocab:
            for w2 in vocab:
                if w1 != w2:
                    df.at[w1, w2] = co_matrix[w1].get(w2, 0)

        # 0이 아닌 동시 출현 쌍만 저장 (long format)
        stacked = df.stack().reset_index()
        stacked.columns = ['word1', 'word2', 'count']
        filtered = stacked[stacked['count'] > 0]

        buffer = StringIO()
        filtered.to_csv(buffer, encoding='cp949', index=False)
        filename = cmn.get_save_filename(cmn.AppType.CONCOR)
        filename = filename.replace(".json", ".csv")
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)
        query.update_state_to_completed(task_no)     

if __name__ == "__main__":
    app = ConcorApplication()
    app.execute({"id": 3, "prev_task_id": 3})

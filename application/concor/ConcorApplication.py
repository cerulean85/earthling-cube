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
        clean_task_id = task["clean_task_id"]
        query = QueryPipeTaskClean()
        searched = query.get_task_by_id(clean_task_id)

        query = QueryPipeTaskConcor()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)

        resource_origin = read_file_from_s3(searched.s3_url)
        data = json.loads(resource_origin)

        co_matrix = defaultdict(lambda: defaultdict(int))
        vocab = set()
        word_pos_map = {}
        word_count = defaultdict(int)  # 각 단어의 개별 출현 횟수

        # 각 문장에서 단어들 추출 및 조합 (품사 정보 포함)
        for item in data:
            words = []
            for token in item['tokens']:
                word = token['word']
                pos = token['pos']
                if word and len(word.strip()) > 0:  # 빈 단어 제외
                    words.append(word)
                    word_pos_map[word] = pos  # 단어-품사 매핑 저장
            
            vocab.update(words)
            # 각 단어의 개별 출현 횟수 카운트 (문장 내 중복 제거)
            for word in set(words):
                word_count[word] += 1
            
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

        # 0이 아닌 동시 출현 쌍만 저장 (long format with POS info)
        stacked = df.stack().reset_index()
        stacked.columns = ['word1', 'word2', 'count']
        filtered = stacked[stacked['count'] > 0]
        
        # 순서쌍 제거 - word1 < word2 조건으로 중복 제거
        filtered = filtered[filtered['word1'] < filtered['word2']]
        
        # 품사 정보 추가
        filtered['pos1'] = filtered['word1'].map(word_pos_map)
        filtered['pos2'] = filtered['word2'].map(word_pos_map)
        
        # 각 단어의 개별 출현 횟수 추가
        filtered['word1Frequency'] = filtered['word1'].map(word_count)
        filtered['word2Frequency'] = filtered['word2'].map(word_count)
        
        # 컬럼 순서 재정렬
        filtered = filtered[['word1', 'pos1', 'word1Frequency', 'word2', 'pos2', 'word2Frequency', 'count']]
        
        # count별 상위 100개만 선택
        top_100 = filtered.nlargest(100, 'count')

        buffer = StringIO()
        # NDJSON 형태로 저장 (각 행을 JSON 객체로)
        for _, row in top_100.iterrows():
            json_line = {
                "word1": row['word1'],
                "pos1": row['pos1'],
                "word1Frequency": int(row['word1Frequency']),
                "word2": row['word2'], 
                "pos2": row['pos2'],
                "word2Frequency": int(row['word2Frequency']),
                "count": int(row['count'])
            }
            buffer.write(json.dumps(json_line, ensure_ascii=False) + '\n')
        
        filename = cmn.get_save_filename(cmn.AppType.CONCOR)
        filename = filename.replace(".json", ".ndjson")
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)
        query.update_state_to_completed(task_no)     

if __name__ == "__main__":
    app = ConcorApplication()
    app.execute({"id": 22, "clean_task_id": 24})

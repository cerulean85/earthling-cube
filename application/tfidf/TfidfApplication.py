from io import StringIO
import json, sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import QueryPipeTaskClean, QueryPipeTaskTfidf
from earthling.connector.s3_module import read_file_from_s3
from sklearn.feature_extraction.text import TfidfVectorizer

class TfidfApplication:
    def execute(self, task):
        clean_task_id = task["clean_task_id"]
        query = QueryPipeTaskClean()
        searched = query.get_task_by_id(clean_task_id)

        query = QueryPipeTaskTfidf()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)

        resource_origin = read_file_from_s3(searched.s3_url)
        converted = json.loads(resource_origin)
        morphs_list = cmn.convert_json_to_morph(converted)

        # 품사별로 단어를 분리하여 저장
        word_pos_map = {}
        documents = []
        
        for morphs in morphs_list:
            # 단어만 추출하여 TF-IDF 계산 (품사는 나중에 매핑)
            words = []
            for morph in morphs:
                word = morph[0]
                pos = morph[1]
                if word and len(word.strip()) > 0:  # 빈 단어 제외
                    words.append(word)
                    # 단어와 품사 매핑 저장
                    word_pos_map[word] = pos
            
            if words:  # 빈 문서 방지
                joined = ' '.join(words)
                documents.append(joined)

        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(documents)
      
        features = vectorizer.get_feature_names_out()
        measure_list = []
        top_n = 10

        for row in tfidf_matrix.toarray():
            top_indices = row.argsort()[::-1][:top_n]
            top_features = []
            for idx in top_indices:
                if row[idx] > 0:
                    word = features[idx]
                    pos = word_pos_map.get(word, 'UNK')  # 매핑에서 품사 가져오기
                    top_features.append({ 
                        "word": word,
                        "pos": pos,
                        "value": round(row[idx], 6)
                    })
            measure_list.append(top_features)
        
        buffer = StringIO()
        filename = cmn.get_save_filename(cmn.AppType.TFIDF)
        json.dump(measure_list, buffer, ensure_ascii=False, indent=2)

        query.update_state_to_completed(task_no)
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)        

if __name__ == "__main__":
    app = TfidfApplication()
    app.execute({"id": 24, "clean_task_id": 26})

from io import StringIO
import json, sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import QueryPipeTaskClean, QueryPipeTaskTfidf
from earthling.connector.s3_module import read_file_from_s3
from sklearn.feature_extraction.text import TfidfVectorizer

class TfidfApplication:
    def execute(self, task):
        prev_task_id = task["prev_task_id"]
        query = QueryPipeTaskClean()
        searched = query.get_task_by_id(prev_task_id)

        query = QueryPipeTaskTfidf()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)

        resource_origin = read_file_from_s3(searched.s3_url)
        converted = json.loads(resource_origin)
        morphs_list = cmn.convert_json_to_morph(converted)

        documents = []
        for morphs in morphs_list:
            filtered_words = [morph[0] for morph in morphs]
            joined = ' '.join(filtered_words)
            documents.append(joined)

        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(documents)
      
        words = vectorizer.get_feature_names_out()
        measure_list = []
        top_n = 10

        for row in tfidf_matrix.toarray():
            top_indices = row.argsort()[::-1][:top_n]
            top_word = [{ 
                "word": words[idx],
                "value": round(row[idx], 6)
            } for idx in top_indices if row[idx] > 0]
            measure_list.append(top_word)
        
        buffer = StringIO()
        filename = cmn.get_save_filename(cmn.AppType.TFIDF)
        json.dump(measure_list, buffer, ensure_ascii=False, indent=2)

        query.update_state_to_completed(task_no)
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)        

if __name__ == "__main__":
    app = TfidfApplication()
    app.execute({"id": 1, "prev_task_id": 1})

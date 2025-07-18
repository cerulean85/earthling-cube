from collections import Counter
from io import StringIO
import json, sys, os, ndjson
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import QueryPipeTaskClean, QueryPipeTaskFrequency
from earthling.connector.s3_module import read_file_from_s3

class FrequencyApplication:
    def execute(self, task):
        clean_task_id = task["clean_task_id"]
        query = QueryPipeTaskClean()
        searched = query.get_task_by_id(clean_task_id)

        query = QueryPipeTaskFrequency()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)
        
        resource_origin = read_file_from_s3(searched.s3_url)
        converted_to_json = json.loads(resource_origin)
        converted = cmn.convert_json_to_morph(converted_to_json)

        merged = [tuple(item) for sublist in converted for item in sublist]
        freq_table = Counter(merged).most_common()
        freq_list = []
        for (word, pos), count in dict(freq_table).items():
            freq_list.append({ "word": word, "pos": pos, "count": count })
        
        top_n = 100
        filename = cmn.get_save_filename(cmn.AppType.FREQUENCY)
        filename = filename.replace(".json", ".ndjson")
        buffer = StringIO()
        ndjson.dump(freq_list[:top_n], buffer, ensure_ascii=False)

        query.update_state_to_completed(task_no)
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)

if __name__ == "__main__":
    app = FrequencyApplication()
    app.execute({"id": 26, "clean_task_id": 26})





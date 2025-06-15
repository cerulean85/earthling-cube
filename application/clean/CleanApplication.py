from io import StringIO
import json, sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import PipeTaskStatus, QueryPipeTaskClean, QueryPipeTaskSearch
from earthling.connector.s3_module import read_file_from_s3
from mecab import MeCab

class CleanApplication:
    def execute(self, task):
        prev_task_id = task["prev_task_id"]
        query = QueryPipeTaskSearch()
        searched = query.get_task_by_id(prev_task_id)
        origin_url = searched.s3_url

        query = QueryPipeTaskClean()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)
        resource_origin = read_file_from_s3(origin_url)

        target_list = []
        resource_list = resource_origin.split("\n")
        for resource in resource_list:
            line = resource.split("\t")
            if len(line) < 2:
                continue
            
            title, text = line[0], line[2]
            target_text = f"{title} {text}"
            target_list.append(target_text)

        tagger = MeCab()
        target_pos = {"NNG", "NNP", "NNB", "VV", "VA"}
        pos_filited_list = []
        for text in target_list:
            pos_list = tagger.pos(text)
            filtered = [[word, pos] for word, pos in pos_list if pos in target_pos]
            pos_filited_list.append(filtered)
        morph_data = [[list(pair) for pair in sublist] for sublist in pos_filited_list]

        converted = cmn.convert_morph_to_json(morph_data)
        filename = cmn.get_save_filename(cmn.AppType.CLEAN)
        buffer = StringIO()
        json.dump(converted, buffer, ensure_ascii=False, indent=2)
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)
        query.update_state_to_completed(task_no)

        query.update_state_to_pending_about_other_task(PipeTaskStatus.FREQUENCY, task_no)
        query.update_state_to_pending_about_other_task(PipeTaskStatus.TFIDF, task_no)
        query.update_state_to_pending_about_other_task(PipeTaskStatus.CONCOR, task_no)

if __name__ == "__main__":
    app = CleanApplication()
    app.execute({"id": 1, "prev_task_id": 3})

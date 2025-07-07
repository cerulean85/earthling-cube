# from application.clean.CleanApplication import CleanApplication
from application.search.google.GooglePortal import GooglePortal
if __name__ == "__main__":
    google = GooglePortal()
    google.search(
        "코로나",
        1,
        stop=0,
        date_start="2023-01-01",
        date_end="2023-01-31",
        num=1,
        out_filepath="test.txt",
    )

    # app = CleanApplication()
    # app.execute({"id": 24, "search_task_id": 62})
from application.clean.CleanApplication import CleanApplication

if __name__ == "__main__":
    app = CleanApplication()
    app.execute({"id": 24, "search_task_id": 62})
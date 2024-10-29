from guts_utils.guts_task import guts_task
from typing import Optional
import uuid
import sqlite3

class guts_queue:
    def __init__(self, db_name : Optional[str] = 'guts_queue.db'):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        """ Initialize the task table and counter table """
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter('GUID', lambda b: uuid.UUID(bytes_le=b))
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Create tasks table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid GUID  NOT NULL DEFAULT NONE,
            dep GUID NOT NULL DEFAULT NONE,
            task_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        ''')

        # Create a table to track completed tasks
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS task_counter (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            completed_tasks INTEGER DEFAULT 0
        )
        ''')

        # Ensure the counter row is initialized (there will be only one row with id=1)
        cursor.execute('INSERT OR IGNORE INTO task_counter (id, completed_tasks) VALUES (1, 0)')

        # Todo: 
        # Create an events queue

        conn.commit()
        conn.close()

    def _connect(self):
        """ Create a new SQLite connection for each process """
        return sqlite3.connect(self.db_name)

    def add_task(self, task, deps = None) -> uuid.UUID:
        """ Add a new task to the queue """
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter('GUID', lambda b: uuid.UUID(bytes_le=b))
        conn = self._connect()
        cursor = conn.cursor()
        t_uuid = uuid.uuid4()
        if deps:
            cursor.execute("SELECT id FROM tasks WHERE uuid = ?", (deps,))
            data = cursor.fetchone()
            if data is None:
                print("Can't add dependencies to a non-existing task")
            cursor.execute('INSERT INTO tasks (uuid, dep, task_json, status) VALUES (?, ?, ?, ?)',
                           (t_uuid, deps, task.to_json(), 'pending'))
        else:
            cursor.execute('INSERT INTO tasks (uuid, task_json, status) VALUES (?, ?, ?)',
                           (t_uuid, task.to_json(), 'pending'))
        conn.commit()
        conn.close()
        return t_uuid

    def fetch_task(self):
        """ Fetch the next pending task and mark it as 'in_progress' """
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter('GUID', lambda b: uuid.UUID(bytes_le=b))
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('SELECT id, uuid, dep, task_json FROM tasks WHERE status = ? ORDER BY id LIMIT 1', ('pending',))
        task_data = cursor.fetchone()

        if task_data:
            task_id, task_uuid, task_json = task_data
            cursor.execute('UPDATE tasks SET status = ? WHERE id = ?', ('in_progress', task_id))
            conn.commit()
            conn.close()
            return task_id, task_uuid, Task.from_json(task_json)
        conn.close()
        return None

    def mark_task_done(self, task_id):
        """ Mark the task as done """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE tasks SET status = ? WHERE id = ?', ('done', task_id))
        conn.commit()
        conn.close()

    def increment_completed_tasks(self):
        """ Atomically increment the completed tasks counter and fetch its new value """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE task_counter SET completed_tasks = completed_tasks + 1 WHERE id = 1')
        cursor.execute('SELECT completed_tasks FROM task_counter WHERE id = 1')
        completed_tasks = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return completed_tasks

    def get_completed_tasks(self):
        """ Retrieve the current value of the completed tasks counter """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('SELECT completed_tasks FROM task_counter WHERE id = 1')
        completed_tasks = cursor.fetchone()[0]
        conn.close()
        return completed_tasks

    def get_running_tasks_count(self):
        """ Return the number of tasks marked in-progress """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT() FROM tasks WHERE status = "in_progress"')
        count = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return count

    def get_tasks_count(self):
        """ Return the total number of tasks in the queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT() FROM tasks')
        count = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return count

    def dump_json(self):
        """ Dump the content of the DB to a json file """
        conn = self._connect()
        cursor = conn.cursor()
        tasks = cursor.execute('SELECT * FROM tasks').fetchall()
        for t in tasks:
            print(t)
            #if t[2] == -1:
            #    print([t[0],uuid.UUID(bytes_le=t[1]), t[2], t[3], t[4]])
            #else:
            #    print([t[0],uuid.UUID(bytes_le=t[1]), uuid.UUID(bytes_le=t[2]), t[3], t[4]])
        conn.commit()
        conn.close()

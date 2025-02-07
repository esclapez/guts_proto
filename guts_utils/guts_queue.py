from guts_utils.guts_task import guts_task
from guts_utils.guts_event import guts_event
from guts_utils.guts_resource_manager import resource_set_baseclass
from typing import Optional, Any
import uuid
import time
import os
import sqlite3

class guts_queue:
    """A disk-based SQL queue for GUTS.

    The GUTS queue is a lightweight class interfacing with a
    disk-based SQL queue. It provides GUTS core scheduler
    components (workers, workergroup, resource manager, tasks, ...)
    an atomic process to interact with the flow of GUTS.
    """
    def __init__(self,
                 db_name : Optional[str] = 'guts_queue.db'):
        """Initialize the GUTS queue

        Arguments
        ---------
        str
            The queue file name [Optional]
        """
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        """Initialize the content of the queue.


        Raises
        ------
        RuntimeError
            If a connection to the DB could not be acquired
        """
        conn = self._connect()
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
        # TODO: might be useless
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS task_counter (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            completed_tasks INTEGER DEFAULT 0
        )
        ''')

        # Ensure the counter row is initialized (there will be only one row with id=1)
        cursor.execute('INSERT OR IGNORE INTO task_counter (id, completed_tasks) VALUES (1, 0)')

        # Create an events queue
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acc_count INTEGER DEFAULT 0,
            event_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        ''')

        # Create a worker register
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS workers (
            id INTEGER NOT NULL,
            gid INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'waiting'
        )
        ''')

        # Create a worker group register
        # status
        # 'pending'
        #   Upon requesting resources for a worker group.
        # 'active'
        #   When worker group has initiated his workers
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS worker_groups (
            id INTEGER NOT NULL,
            resource_set_json TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
        )
        ''')

        conn.commit()
        conn.close()

    def _connect(self, isolation_level : Optional[str] = "DEFERRED"):
        """ Create a new SQLite connection for each process """
        # Append uuid to SQL supported types
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter('GUID', lambda b: uuid.UUID(bytes_le=b))
        try:
            return sqlite3.connect(self.db_name, isolation_level = isolation_level)
        except Exception:
            print(f"Unable to connect to {self.db_name}")
            raise

    def _exec_sql_getone(self, sql_cmd : str, args : Optional[tuple] = None) -> Optional[Any]:
        """ Execute an SQL command """
        conn = self._connect()
        cursor = conn.cursor()
        if args is None:
            res = cursor.execute(sql_cmd).fetchone()[0]
        else:
            res = cursor.execute(sql_cmd, args).fetchone()[0]
        conn.commit()
        conn.close()
        return res

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

    def add_event(self, event):
        """ Add a new event to the queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO events (event_json, status) VALUES (?, ?)',
                       (event.to_json(), 'pending'))
        conn.commit()
        conn.close()

    def fetch_task(self):
        """ Fetch the next pending task and mark it as 'in_progress' """
        sqlite3.register_adapter(uuid.UUID, lambda u: u.bytes_le)
        sqlite3.register_converter('GUID', lambda b: uuid.UUID(bytes_le=b))
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
        cursor.execute('SELECT id, uuid, dep, task_json FROM tasks WHERE status = ? ORDER BY id LIMIT 1', ('pending',))
        task_data = cursor.fetchone()

        if task_data:
            task_id, task_uuid, task_dep, task_json = task_data
            cursor.execute('UPDATE tasks SET status = ? WHERE id = ?', ('in_progress', task_id))
            conn.commit()
            conn.close()
            return task_id, task_uuid, guts_task.from_json(task_json)
        conn.close()
        return None

    def mark_task_done(self, task_uuid):
        """ Mark the task as done """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE tasks SET status = ? WHERE uuid = ?', ('done', task_uuid))
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

    def get_completed_tasks(self) -> int:
        """ Retrieve the current value of the completed tasks counter """
        return self._exec_sql_getone('SELECT completed_tasks FROM task_counter WHERE id = 1')

    def get_running_tasks_count(self) -> int:
        """ Return the number of tasks marked in-progress """
        return self._exec_sql_getone('SELECT COUNT() FROM tasks WHERE status = "in_progress"')

    def get_remaining_tasks_count(self) -> int:
        """ Return the number of tasks marked pending/in-progress """
        return self._exec_sql_getone('SELECT COUNT() FROM tasks WHERE status IN ("pending", "in_progress")')

    def get_tasks_count(self) -> int:
        """ Return the total number of tasks in the queue """
        return self._exec_sql_getone('SELECT COUNT() FROM tasks')

    def get_events_count(self) -> int:
        """ Return the total number of events in the queue """
        return self._exec_sql_getone('SELECT COUNT() FROM events')

    def fetch_event(self) -> Optional[tuple[int, int, guts_event]]:
        """ Fetch the next pending event """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('SELECT id, acc_count, event_json, status FROM events WHERE status = ? ORDER BY id LIMIT 1', ('pending',))
        event_data = cursor.fetchone()

        if event_data:
            event_id, acc_count, event_json, status = event_data
            acc_count = acc_count + 1
            cursor.execute('UPDATE events SET acc_count = ? WHERE id = ?', (acc_count, event_id))
            conn.commit()
            conn.close()
            return event_id, acc_count, guts_event.from_json(event_json)
        conn.close()
        return None

    def register_worker(self, wid : tuple[int,int]) -> None:
        """ Register a worker in the queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO workers (gid, id) VALUES (?, ?)', (wid[0], wid[1]))
        conn.commit()
        conn.close()

    def unregister_worker(self, wid : tuple[int,int]) -> None:
        """ Unregister a worker from the queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM workers WHERE gid = ? AND id = ?', (wid[0], wid[1]))
        conn.commit()
        conn.close()

    def update_worker_status(self, wid : tuple[int,int], status : str) -> None:
        """ Update the worker status in queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE workers SET status = ? WHERE gid = ? AND id = ?', (status, wid[0], wid[1]))
        conn.commit()
        conn.close()

    def get_workers_count(self) -> int:
        """ Return the number of workers """
        return self._exec_sql_getone('SELECT COUNT() FROM workers')

    def get_active_workers_count(self) -> int:
        """ Return the number of active workers """
        return self._exec_sql_getone('SELECT COUNT() FROM workers WHERE status = "working"')

    def register_worker_group(self,
                              gid : int,
                              status : Optional[str] = None) -> None:
        """ Register a worker group in the queue """
        conn = self._connect()
        cursor = conn.cursor()
        if status is None:
            cursor.execute('INSERT INTO worker_groups(id) VALUES (?)', (gid,))
        else:
            cursor.execute('INSERT INTO worker_groups(id, status) VALUES (?,?)', (gid,status))
        conn.commit()
        conn.close()

    def unregister_worker_group(self,
                                gid : int) -> None:
        """ Unregister a worker group from the queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM worker_groups WHERE id = ?', (gid,))
        conn.commit()
        conn.close()

    def check_worker_group(self, gid : int) -> str:
        """Return the status of a given worker group."""
        return self._exec_sql_getone('SELECT status FROM worker_groups WHERE id = ?', (gid,))

    def update_worker_group_status(self,
                                   gid : int,
                                   status : str) -> None:
        """ Update the worker group status in queue """
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE worker_groups SET status = ? WHERE id = ?', (status, gid))
        conn.commit()
        conn.close()

    def update_worker_group_resources(self,
                                      gid : int,
                                      resource : str) -> None:
        """Update the worker group resources set in queue."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('UPDATE worker_groups SET resource_set_json = ? WHERE id = ?', (resource, gid))
        conn.commit()
        conn.close()

    def get_worker_group_resource(self,
                                  gid : int) -> str | None:
        """Query the queue to get a given workergroup resource."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute('SELECT resource_set_json, status FROM worker_groups WHERE id = ?', (gid,))
        wgroup_data = cursor.fetchone()

        if wgroup_data:
            resource_set, status = wgroup_data
            cursor.execute('UPDATE worker_groups SET status = ? WHERE id = ?', ('active', gid))
            conn.commit()
            conn.close()
            return resource_set
        conn.close()
        return None

    def get_worker_groups_count(self) -> int:
        """ Return the number of worker groups """
        return self._exec_sql_getone('SELECT COUNT() FROM worker_groups')

    def dump_tasks_json(self) -> None:
        """ Dump the content of the task table to a json file """
        conn = self._connect()
        cursor = conn.cursor()
        tasks = cursor.execute('SELECT * FROM tasks').fetchall()
        for t in tasks:
            if t[2] != "NONE":
                print([t[0],uuid.UUID(bytes_le=t[1]), uuid.UUID(bytes_le=t[2]), t[3], t[4]])
            else:
                print([t[0],uuid.UUID(bytes_le=t[1]), t[2], t[3], t[4]])
        conn.commit()
        conn.close()

    def dump_events_json(self) -> None:
        """ Dump the content of the event table to a json file """
        conn = self._connect()
        cursor = conn.cursor()
        events = cursor.execute('SELECT * FROM events').fetchall()
        for e in events:
            print(e)
        conn.commit()
        conn.close()

    def delete(self,
               wait_for_done : bool = True,
               timeout : int = 60) -> None:
        """Delete the DB when all tasks and workers are done"""
        # Initialize time for timeout
        time_st = time.time()

        # Trigger a kill all event
        self.add_event(guts_event(eid = 1, action ="worker-kill", target = "all"))

        # Wait until no more tasks/workers active or timeout
        while ((self.get_remaining_tasks_count() > 0 
            or self.get_workers_count() > 0 ) 
            and time.time() - time_st < timeout):
            time.sleep(0.1)

        # Actually delete the DB
        os.remove(self.db_name)

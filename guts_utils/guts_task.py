"""A plain class to represent GUTS tasks."""
import json

# Task class to represent a callable function with arguments
class guts_task:
    def __init__(self, function_name, args=None):
        self.function_name = function_name  # String name of the function to call
        self.args = args if args is not None else {}

    def to_json(self):
        """ Serialize the task to a JSON string for storage """
        return json.dumps({
            'function_name': self.function_name,
            'args': self.args
        })

    @staticmethod
    def from_json(task_json):
        """ Deserialize a task from a JSON string """
        task_dict = json.loads(task_json)
        return guts_task(task_dict['function_name'], task_dict['args'])

    def execute(self, task_functions):
        """ Execute the task by calling the corresponding function """
        func = task_functions.get(self.function_name)
        if func is None:
            raise ValueError(f"Function '{self.function_name}' is not registered.")
        
        func(**self.args)

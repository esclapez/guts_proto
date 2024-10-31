"""A plain class to represent GUTS tasks."""
import json
import time

# A register of task functions
task_functions_reg = {}

def register_taskf(function_name):
    def decorator(function):
        task_functions_reg[function_name] = function
        return function
    return decorator

def unregister_taskf(function_name):
    if function_name in task_functions:
        del task_functions_reg[function_name]

@register_taskf('function_test')
def function_test(nap_duration : int = 1) -> None:
    #print("Function test waits")
    time.sleep(nap_duration)
    #print("Function test wrapping up")

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

    def execute(self):
        """ Execute the task by calling the corresponding function """
        func = task_functions_reg.get(self.function_name)
        if func is None:
            raise ValueError(f"Function '{self.function_name}' is not registered.")
        
        func(**self.args)

import logging
import time

class Pipeline(object):

    def __init__(self):
        self.tasks = []


    def _load_wrapper(self, func, values):
        """
        Handles the execution of each step in the process. Most
        of the complexity relates to handling of multiple outputs
        from a single input.

        Should not be called directly.
        """
        logging.info('\'load\' is linear and doesn\'t scale well, this probably isn\'t what you want.')
        if values == None:
            return
        if type(values).__name__ in ['generator']:
            for value in [value for value in values if value != None]:
                return_values = func(value)
                yield from return_values
        else:
            raise Exception('tasks should return None or a Generator (use yield) {}'.format(values))


    def load(self, values):
        """
        Execute the pipeline.

        This executes the entire pipeline for a step at a time.
        This starts to cause problems on very large datasets.
        """
        for task in self.tasks:
            values = self._load_wrapper(task.execute, values)
        yield from values


    def _run_wrapper(self, value):
        values = [value]
        for task in self.tasks:
            start_ns = time.time_ns()
            next_set = []
            for value in values:
                res = task.execute(value)
                next_set = next_set + list(res)
            values = next_set
            task.execution_time += time.time_ns() - start_ns
        yield from values

    def run(self, values):
        for value in values:
            yield from self._run_wrapper(value)


    def add_process(self, task):
        """
        Add a process to the pipeline.

        Adds to the end of the existing pipeline, calls the 'init'
        method on the Process.
        """
        if not hasattr(task, 'execute'):
            raise Exception('A Pipeline Process was added that is missing the required method \'execute\'.')
        if hasattr(task, 'init'):
            task.init()
        self.tasks.append(task)


    def close(self):
        """
        Call the close methods for each of the Processes
        """
        for task in self.tasks:
            if hasattr(task, 'close'):
                task.close()


    def sensors(self):
        for task in self.tasks:
            if hasattr(task, 'read_sensor'):
                print(task.read_sensor())
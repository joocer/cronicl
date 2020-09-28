import logging

class Pipeline(object):

    def __init__(self):
        self.tasks = []


    def _wrapper(self, func, values):
        """
        Handles the execution of each step in the process. Most
        of the complexity relates to handling of multiple outputs
        from a single input.

        Should not be called directly.
        """
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

        This executes the entire pipeline for a record at a time.
        """
        for task in self.tasks:
            values = self._wrapper(task.execute, values)
        yield from values


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
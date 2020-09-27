

class Pipeline(object):

    def __init__(self):
        self.tasks = []

    def run(self, value):
        for task in self.tasks:
            value = task.execute(value)
            if value == None:
                break
        return value

    def add_process(self, task):
        if not hasattr(task, 'execute'):
            raise Exception('A Pipeline Process was added that is missing the required method \'execute\'.')
        if hasattr(task, 'init'):
            task.init()
        self.tasks.append(task)

    def audits(self):
        for task in self.tasks:
            if hasattr(task, 'get_audit_detatils'):
                print(task.get_audit_detatils())
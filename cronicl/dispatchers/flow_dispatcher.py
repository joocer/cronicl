from .base_dispatcher import BaseDispatcher


class FlowDispatcher(BaseDispatcher):
    """
    Dispatcher
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flow = kwargs.pop("flow")
        self.label = kwargs.get("label")
        if not self.label:
            self.label = self.flow.label

    def on_event(self, payload):
        self.flow.execute(payload)
        self.on_completion(None)

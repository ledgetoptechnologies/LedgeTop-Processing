class FrameRefused(RuntimeError):
    """A refusal with a stable code. RuntimeError, so the engine stops on it."""

    def __init__(self, code, details=None):
        super().__init__(code)
        self.code = code
        self.details = details or {}

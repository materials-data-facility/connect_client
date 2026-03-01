"""Custom exceptions for MDF Agent."""


class MDFError(Exception):
    """Base exception for MDF Agent errors."""

    pass


class NotARepositoryError(MDFError):
    """Raised when a command requires an MDF repository but none exists."""

    def __init__(self, path: str = "."):
        self.path = path
        super().__init__(f"Not an MDF repository (no mdf.yaml found in a git repo)")

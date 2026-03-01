"""Custom exceptions for MDF Agent."""


class MDFError(Exception):
    """Base exception for MDF Agent errors."""

    pass


class NoManifestError(MDFError):
    """Raised when a command requires an mdf.yaml manifest but none exists."""

    def __init__(self, path: str = "."):
        self.path = path
        super().__init__(f"No mdf.yaml found in {path}")

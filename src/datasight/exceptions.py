"""
Custom exceptions for datasight.

Provides a consistent error handling hierarchy for the application.
"""


class DatasightError(Exception):
    """Base exception for all datasight errors."""

    pass


class ConfigurationError(DatasightError):
    """Raised when configuration is missing or invalid."""

    pass


class DatabaseError(DatasightError):
    """Base exception for database-related errors."""

    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails or is closed."""

    pass


class QueryError(DatabaseError):
    """Raised when a SQL query fails to execute."""

    pass


class ValidationError(DatasightError):
    """Raised when input validation fails."""

    pass


class SQLValidationError(ValidationError):
    """Raised when SQL validation against schema fails."""

    def __init__(self, message: str, errors: list[str] | None = None):
        super().__init__(message)
        self.errors = errors or []


class LLMError(DatasightError):
    """Base exception for LLM-related errors."""

    pass


class LLMConnectionError(LLMError):
    """Raised when connection to LLM provider fails."""

    pass


class LLMResponseError(LLMError):
    """Raised when LLM response is invalid or unexpected."""

    pass


class ProjectError(DatasightError):
    """Raised when project loading or validation fails."""

    pass


class SessionError(DatasightError):
    """Raised when session handling fails."""

    pass


class InvalidSessionIdError(SessionError):
    """Raised when session ID is invalid (e.g., path traversal attempt)."""

    pass

"""Custom exceptions for DuckLake pipeline operations."""


class DucklakeError(Exception):
    """Base exception for all DuckLake operations."""
    pass


class DataIngestionError(DucklakeError):
    """Raised when data ingestion fails."""
    pass


class SchemaValidationError(DucklakeError):
    """Raised when schema validation fails."""
    pass


class FileProcessingError(DucklakeError):
    """Raised when file processing operations fail."""
    pass


class DatabaseOperationError(DucklakeError):
    """Raised when database operations fail."""
    pass


class EnrichmentError(DucklakeError):
    """Raised when data enrichment operations fail."""
    pass


class ReportGenerationError(DucklakeError):
    """Raised when report generation fails."""
    pass
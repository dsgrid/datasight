"""datasight — AI-powered database exploration with natural language."""

# Use the OS certificate store so HTTPS works behind corporate proxies,
# but only if truststore is installed (it's a core dependency but guard
# against edge cases where it may be missing or unsupported).
try:
    import truststore

    truststore.inject_into_ssl()
    del truststore
except (ImportError, Exception):
    pass

__version__ = "0.2.0"

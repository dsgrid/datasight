"""datasight — AI-powered database exploration with natural language."""

# Use the OS certificate store so HTTPS works behind corporate proxies.
import truststore

truststore.inject_into_ssl()
del truststore

__version__ = "0.2.0"

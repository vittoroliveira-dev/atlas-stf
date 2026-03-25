"""DataJud API client for CNJ public data."""

from ._runner import discover_indices, fetch_origin_data, fetch_single_index

__all__ = ["discover_indices", "fetch_origin_data", "fetch_single_index"]

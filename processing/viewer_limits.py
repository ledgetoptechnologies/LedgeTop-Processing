"""Limits shared by Viewer transport and its advertised capability."""

# Complete multipart request body, including MIME framing and all file bytes.
MAX_UPLOAD_BYTES = 2 * 1024 ** 3

# Free space an eager result fetch leaves alone. The updater's capacity policy
# needs 100 GiB free plus a 64 GiB reservation; this keeps its admission open if
# the state volume shares that pool. Bytes all eager custody may retain in total.
EAGER_CUSTODY_RESERVE_BYTES = 164 * 1024 ** 3
EAGER_CUSTODY_MAX_BYTES = 512 * 1024 ** 3

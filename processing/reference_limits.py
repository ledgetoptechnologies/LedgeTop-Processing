"""Explicit metadata budgets for bounded reference protocol paths."""

# V1 request, pipeline and manifest budgets remain unchanged. V2 source intake
# has a separate bounded envelope and internal canonical manifest budget.
# Prepared-V3/outbox caps remain independent and are not relaxed here.
V2_SOURCE_INTAKE_BYTES = 16 * 1024 * 1024
PRIVATE_CONTROL_RESPONSE_BYTES = 16 * 1024 * 1024

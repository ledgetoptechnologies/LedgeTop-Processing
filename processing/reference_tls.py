"""Verified per-client trust: system roots plus an explicitly configured CA.

File ownership/path validation belongs to the configuration caller. This helper
does not alter environment variables, machine trust, or any other SSL context.
"""
import ssl


def verified_client_context(ca_file):
    """Keep normal system trust and add the private NAS/worker CA, or fail closed."""
    context = ssl.create_default_context()
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.minimum_version = max(context.minimum_version, ssl.TLSVersion.TLSv1_2)
    # Passing cafile to create_default_context would replace normal system roots.
    # Load this additional anchor only after the default store is established.
    context.load_verify_locations(cafile=ca_file)
    return context

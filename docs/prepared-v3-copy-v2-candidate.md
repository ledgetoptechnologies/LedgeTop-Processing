# Unmounted v2 copy integration experiment

This candidate is not registered with the prepared API or any listener. Both
integration and migration readiness are hard false, and migration always
refuses. Tests patch gates and create a fixture-only table in owned temporary
SQLite; those patches are not evidence the gates can safely open.

The new write path checks legacy identities and counts legacy plus v2 data,
but legacy writers know neither the fixture namespace nor its byte usage. A
test deliberately demonstrates that an existing legacy namespace check misses
a fixture receipt ID. A second test demonstrates legacy quota accounting misses
fixture bytes. The presence of these tests is not an authorization to enable.

Before any shared writer can run, a separate design must provide a complete
inventory of every receipt writer and namespace, durable cross-namespace ID
ownership and quota accounting checked by every writer in the same SQLite
transaction, and a deployment/downgrade fence excluding binaries unaware of
that invariant. Schema-version census, migration and rollback, corrupt and
unknown rows, cross-version simultaneous writes, restart and lost-response
cases need independent tests. The isolated candidate does none of that.

The service registry also depends on complete trusted configuration and a
separately authenticated mapping from submission owner to Viewer service. The
fixture's transport flag is an assertion, not TLS or peer authentication.
Storage durability, v2 protocol negotiation, source retention, and custody
acknowledgements remain unqualified; a signed receipt is not cleanup authority.

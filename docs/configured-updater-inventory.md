# Configured-node updater identity

The updater remains one host-local instance of the existing registry, qualifier,
supervisor and maintenance adapter. Node identifiers are configured slugs, not a
fixed main/cluster enumeration. Before discovery can advance, the adapter verifies
that its selected node exists exactly once in the actual coordinator inventory.
The existing authority, engine, managed-root, qualification and physical Gate
bindings remain required. A third configured node does not inherit another node's
identity or receipts.

Updater configuration schema 2 retains every schema-1 component and adds an
explicit `host` object:

```json
{
  "uid": 1000,
  "userName": "operator",
  "hostname": "gpu-third.example",
  "pythonExecutable": "/opt/processing/venv/bin/python",
  "requiresTunnel": true
}
```

These example values are placeholders. The process must match the exact reviewed
UID, username, full lower-case hostname and interpreter path. The selected node
in supervisor and qualification policy must agree. Existing schema-1 host
bindings keep their legacy meaning; an unfamiliar node cannot infer one.

`render_configured_units(source_root, config_path, configuration)` consumes this
same closed schema-2 configuration, rendering the selected dependency interpreter
and an explicit tunnel dependency. It renders text only. It does not install,
enable or start either unit or timer. Legacy `render_units` defaults remain for
the two historical profiles; new profiles must specify both interpreter and
tunnel requirement.

An explicit interpreter path is a binding, not proof of installed dependency
qualification. Before enabling any timer, qualify that interpreter, source and
dependencies on the real host with its actual managed root, coordinator and
worker. No OS updates are included.

Scheduled and after-job drain admission already exists in the coordinator and
manual maintenance CLI. Early owned drain before image qualification is a
separate updater extension; this inventory change alone does not implement or
qualify that reordering.

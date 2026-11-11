# Dashboard projection prototype

`GET /v1/observer/dashboard` requires the distinct `PROCESSING_DASHBOARD_TOKEN` bearer credential. It must not equal any API, operator, legacy observer, Viewer or configured node credential. It grants only this projection and is rejected by legacy routes. The endpoint is disabled when no separate token is configured. Deploy only behind an authenticated TLS boundary; the token is never intended for a browser.

`after` is a creation-sequence cursor, not an update cursor. Each page has a separate database snapshot; pages do not form one atomic fleet snapshot. Restart at `after=0` and read all pages to refresh existing jobs. Concurrent inserts, removals, or filter changes can affect a scan. `observedAt` is the coordinator database read time, not an engine heartbeat. `queueUpdatedAt` is the jobs row timestamp and `viewerUpdatedAt` is the viewer task row timestamp; neither proves current engine or host liveness.

Nodes are listed in configured preference order with coordinator maintenance state, not live engine health or resource usage. `uploadedFiles` counts coordinator-to-NodeODM acknowledged files; `totalFiles` is null until a bounded authoritative count is supplied. This does not report Viewer-to-coordinator transfer progress. Job, provider-task and engine-task identifiers are distinct; no client paths, display names, claims, or result bodies are returned.

Timestamps are explicit UTC ISO 8601. SQLite's exact `YYYY-MM-DD HH:MM:SS[.ffffff]` form is interpreted as UTC because SQLite CURRENT_TIMESTAMP produces UTC; offset-qualified ISO values are converted to UTC. Null remains null. Unknown or invalid stored timestamp formats fail the response closed with `invalid_stored_state`.

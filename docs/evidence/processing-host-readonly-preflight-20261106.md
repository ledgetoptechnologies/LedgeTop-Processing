# Processing host read-only preflight

Fresh authenticated SSH readback on November6 UTC (November5 local), using the
existing bkoltz key and bounded connection timeout. No service, container,
permission, storage or credential settings were changed.

| Observation | Cluster192.168.50.89 | Main192.168.50.88 |
| --- | --- | --- |
| SSH identity | UID1000 | UID1000 |
| Mounted root filesystem | 295GiB total,112GiB available | 1.8TiB total,1.4TiB available |
| Existing user-manager persistence | `Linger=yes` | `Linger=yes` |
| Reference worker unit | Not listed | Not listed |
| Image-update timer | Disabled | Disabled |
| Existing processing container | Prototype coordinator running, Docker healthy | Main NodeODX container running |

The cluster figures describe its currently mounted root filesystem, not the
physical SSD's total capacity or unallocated space. They reinforce keeping the
waiting source spool on NAS with reservations rather than assuming a full
terabyte is available locally. No disk expansion or mount change was attempted.

The exact main-host noninteractive privileged read probe
`sudo -n /usr/bin/python3 -I -B -c "import os; print(os.geteuid())"` refused with
`sudo: a password is required`. SSH access is working; a reviewed privileged
installation step still needs the operator. Do not weaken sudoers or substitute
an unreviewed installation to bypass this boundary. Prepare final immutable
source/config/manifest inputs before asking for that operator action.

Existing unrelated/staging workloads were left intact. Docker running/healthy
state does not establish native task idleness, private worker readiness, update
acceptance or a returned Viewer result. Current host/engine/task/ingress and
physical-lock facts must be rechecked at actual installation/activation.
TrueNAS administration and the new spool mount/ACL remain controlled by Beau;
this readback did not access its administrator shell.

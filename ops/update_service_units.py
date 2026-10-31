"""Render deterministic user units; never install, enable, or start them."""
from pathlib import PurePosixPath
import re


NAME='ledgetop-processing-image-updates'


def _path(value):
    if (type(value) is not str or not re.fullmatch(r'/[A-Za-z0-9_./-]+',value)
            or '..' in PurePosixPath(value).parts or str(PurePosixPath(value))!=value):
        raise ValueError('fixed_literal_linux_path_required')
    return value


def render_units(source_root,config_path,*,node_id):
    source_root,config_path=_path(source_root),_path(config_path)
    if node_id not in ('main','cluster'):raise ValueError('fixed_processing_node_required')
    dependency=' ledgetop-processing-tunnel.service' if node_id=='main' else ''
    requires='Requires=ledgetop-processing-tunnel.service\n' if node_id=='main' else ''
    service=f'''[Unit]
Description=LedgeTop Processing image update supervision
After=network-online.target{dependency}
{requires}
[Service]
Type=oneshot
WorkingDirectory={source_root}
Environment=PYTHONDONTWRITEBYTECODE=1
ExecStart=/usr/bin/python3 -B -m ops.update_service tick --config {config_path}
TimeoutStartSec=7200
Restart=no
UMask=0077
NoNewPrivileges=true
StandardOutput=journal
StandardError=journal
'''
    timer=f'''[Unit]
Description=Recheck pending LedgeTop Processing container updates

[Timer]
OnBootSec=5min
OnUnitInactiveSec=1min
AccuracySec=5s
Unit={NAME}.service

[Install]
WantedBy=timers.target
'''
    return {NAME+'.service':service,NAME+'.timer':timer}

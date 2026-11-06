"""Render deterministic user units; never install, enable, or start them."""
from pathlib import PurePosixPath
import re


NAME='ledgetop-processing-image-updates'


def _path(value):
    if (type(value) is not str or not re.fullmatch(r'/[A-Za-z0-9_./-]+',value)
            or '..' in PurePosixPath(value).parts or str(PurePosixPath(value))!=value):
        raise ValueError('fixed_literal_linux_path_required')
    return value


def render_units(source_root,config_path,*,node_id,python_executable=None,requires_tunnel=None):
    source_root,config_path=_path(source_root),_path(config_path)
    if type(node_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}',node_id):
        raise ValueError('fixed_processing_node_required')
    if python_executable is None or requires_tunnel is None:
        if node_id not in ('main','cluster'):raise ValueError('explicit_configured_node_runtime_required')
        if python_executable is None:python_executable='/usr/bin/python3'
        if requires_tunnel is None:requires_tunnel=node_id=='main'
    python_executable=_path(python_executable)
    if type(requires_tunnel) is not bool:raise ValueError('explicit_tunnel_dependency_required')
    dependency=' ledgetop-processing-tunnel.service' if requires_tunnel else ''
    requires='Requires=ledgetop-processing-tunnel.service\n' if requires_tunnel else ''
    service=f'''[Unit]
Description=LedgeTop Processing image update supervision
After=network-online.target{dependency}
{requires}
[Service]
Type=oneshot
WorkingDirectory={source_root}
Environment=PYTHONDONTWRITEBYTECODE=1
ExecStart={python_executable} -B -m ops.update_service tick --config {config_path}
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


def render_configured_units(source_root,config_path,configuration):
    """Use the same reviewed v2 host/node/interpreter binding as the tick."""
    from ops.update_service import parse_configuration
    parsed=parse_configuration(configuration)
    if parsed.raw['schemaVersion']!=2:raise ValueError('explicit_configured_node_runtime_required')
    host=parsed.raw['host']
    return render_units(source_root,config_path,node_id=parsed.supervisor_policy.node_id,
        python_executable=host['pythonExecutable'],requires_tunnel=host['requiresTunnel'])

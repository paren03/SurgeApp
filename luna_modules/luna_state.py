import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

@dataclass
class LunaState:
    # Control Flags
    stop_requested: bool = False
    warm_reset_count: int = 0
    heartbeat_failure_count: int = 0
    last_heartbeat_write_mono: float = 0.0
    
    # Task Tracking
    active_tasks: Dict[str, Any] = field(default_factory=dict)
    done_tasks_count: int = 0
    failed_tasks_count: int = 0
    
    # Threading
    lock: threading.Lock = field(default_factory=threading.Lock)

# Global singleton
CORE_STATE = LunaState()

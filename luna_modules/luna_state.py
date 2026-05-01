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
    
    def reset(self):
        """Reset the state of LunaState."""
        
        with self.lock:
            self.stop_requested = False
            self.warm_reset_count = 0
            self.heartbeat_failure_count = 0
            self.last_heartbeat_write_mono = 0.0
            self.active_tasks.clear()
            self.done_tasks_count = 0
            self.failed_tasks_count = 0

# Global singleton
CORE_STATE = LunaState()

"""A plain class to represent GUTS events."""
from typing import Optional
from dataclasses import dataclass
import json
import sys

# Event class, triggering specific worker behaviors
@dataclass
class guts_event:
    eid : int
    action : str
    target : Optional[str] = None

    def to_json(self) -> str:
        """ Serialize the event to a JSON string for storage """
        return json.dumps({
            'id': self.eid,
            'action': self.action,
            'target': self.target
        })

    @staticmethod
    def from_json(event_json):
        """ Deserialize an event from a JSON string """
        event_dict = json.loads(event_json)
        return guts_event(event_dict['id'],
                          event_dict['action'],
                          event_dict['target'])

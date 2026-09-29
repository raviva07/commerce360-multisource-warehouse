from __future__ import annotations
from dataclasses import dataclass
from time import perf_counter, sleep
from typing import Callable

@dataclass
class Task:
    name: str
    fn: Callable[[], object]
    depends_on: tuple[str,...]=()
    retries: int=1

class DAGRunner:
    def __init__(self,tasks:list[Task]): self.tasks={t.name:t for t in tasks}; self.events=[]
    def run(self):
        done=set()
        while len(done)<len(self.tasks):
            progressed=False
            for name,t in self.tasks.items():
                if name in done or not set(t.depends_on).issubset(done): continue
                last=None
                for attempt in range(1,t.retries+2):
                    started=perf_counter()
                    try:
                        result=t.fn(); self.events.append({'task':name,'attempt':attempt,'status':'success','seconds':round(perf_counter()-started,4)}); done.add(name); progressed=True; last=None; break
                    except Exception as exc:
                        last=exc; self.events.append({'task':name,'attempt':attempt,'status':'failed','error':str(exc)}); sleep(0.05)
                if last: raise last
            if not progressed and len(done)<len(self.tasks): raise RuntimeError('DAG dependency deadlock')
        return self.events

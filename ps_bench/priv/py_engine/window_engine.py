from erlport.erlterms import Atom
from erlport import erlang
from importlib import import_module

LISTENER = None
PLUGINS = []

def _atom(name: str): return Atom(name.encode())

def start(listener_atom, plugins):
    global LISTENER, PLUGINS
    LISTENER = listener_atom
    PLUGINS = [import_module(f"plugins.{p}") for p in plugins]
    return True

def ingest_window(run_id, win_start_ms, win_map):
    lat = win_map.get(Atom(b'lat_ms'), [])
    size = win_map.get(Atom(b'size_b'), [])
    counts = win_map.get(Atom(b'counts'), {})
    summary = {}
    for p in PLUGINS:
        summary.update(p.apply(lat, size, counts))
    erlang.cast(LISTENER, (_atom('win_summary'), run_id, win_start_ms, summary))
    return True

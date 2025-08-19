# priv/py_engine/window_engine.py
from erlport.erlterms import Atom
from erlport import erlang
from importlib import import_module
import os, sys, json

# Make sure we can import the plugins package at priv/plugins
_THIS_DIR = os.path.dirname(__file__)
_PRIV_DIR = os.path.dirname(_THIS_DIR)
if _PRIV_DIR not in sys.path:
    sys.path.append(_PRIV_DIR)

LISTENER = None
PLUGINS = []
OUT_DIR = "/app/out"  
def _atom(name: str):
    return Atom(name.encode())

def _norm_run_id(run_id):
    # ErlPort may give bytes/str/int store as readable string
    if isinstance(run_id, bytes):
        try:
            return run_id.decode("utf-8")
        except Exception:
            return run_id.decode("latin-1", "ignore")
    return str(run_id)

def _append_json(run_id, win_start_ms, summary):
    os.makedirs(OUT_DIR, exist_ok=True)
    rid = _norm_run_id(run_id)
    path = os.path.join(OUT_DIR, f"windows_{rid}.jsonl")
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({
            "run_id": rid,
            "win_start_ms": win_start_ms,
            "summary": summary
        }) + "\n")

def start(listener_atom, plugins, out_dir=None):
    global LISTENER, PLUGINS, OUT_DIR
    LISTENER = listener_atom
    if out_dir:
        OUT_DIR = out_dir
    os.makedirs(OUT_DIR, exist_ok=True)

    # plugin names come from Erlang as binaries; convert to strings
    plugins = list(map(lambda p: p.decode("utf-8"), plugins))
    PLUGINS = [import_module(f"plugins.{p}") for p in plugins]

    # let plugins know where to write files if they define init(out_dir)
    for p in PLUGINS:
        init_fn = getattr(p, "init", None)
        if callable(init_fn):
            init_fn(OUT_DIR)

    return Atom(b"ok")

def ingest_window(run_id, win_start_ms, win_map):
    lat   = win_map.get(Atom(b"lat_ms"), [])
    size  = win_map.get(Atom(b"size_b"), [])
    counts = win_map.get(Atom(b"counts"), {})

    summary = {}
    for p in PLUGINS:
        apply_fn = getattr(p, "apply", None)
        if callable(apply_fn):
            # expect dict back merge into summary
            summary.update(apply_fn(lat, size, counts))

    # 1) send back to Erlang listener (existing behavior)
    erlang.cast(LISTENER, (_atom("win_summary"), run_id, win_start_ms, summary))

    # 2) also write each window’s summary as a JSONL row in OUT_DIR
    _append_json(run_id, win_start_ms, summary)

    return Atom(b"ok")

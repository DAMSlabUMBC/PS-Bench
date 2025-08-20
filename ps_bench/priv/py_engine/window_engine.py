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

def _to_str(x):
    # Normalize Erlang terms (Atom, binary, charlist) to a Python str
    if isinstance(x, (bytes, bytearray, Atom)):
        return bytes(x).decode("utf-8", "ignore")
    if isinstance(x, list) and all(isinstance(i, int) for i in x):  # Erlang charlist
        return bytes(x).decode("utf-8", "ignore")
    return x if isinstance(x, str) else str(x)

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

    # Ensure OUT_DIR is a real str (not bytes/charlist)
    if out_dir is not None:
        OUT_DIR = _to_str(out_dir)
    else:
        OUT_DIR = _to_str(OUT_DIR)

    os.makedirs(OUT_DIR, exist_ok=True)

    # Ensure plugin names are str as well
    plugins = [_to_str(p) for p in plugins]
    PLUGINS = [import_module(f"plugins.{p}") for p in plugins]

    # let plugins know where to write files if they define init(out_dir)
    for p in PLUGINS:
        init_fn = getattr(p, "init", None)
        if callable(init_fn):
            init_fn(OUT_DIR)

    return _atom("ok")

def ingest_window(run_id, win_start_ms, win_map):
    lat   = win_map.get(Atom(b"lat_ms"), [])
    size  = win_map.get(Atom(b"size_b"), [])
    counts = win_map.get(Atom(b"counts"), {})

    # Add win_start_ms so plugins can log it without guessing key types
    counts2 = dict(counts)
    counts2[Atom(b"win_start_ms")] = win_start_ms

    summary = {}
    for p in PLUGINS:
        apply_fn = getattr(p, "apply", None)
        if callable(apply_fn):
            summary.update(apply_fn(lat, size, counts2))

    erlang.cast(LISTENER, (_atom("win_summary"), run_id, win_start_ms, summary))
    _append_json(run_id, win_start_ms, summary)
    return Atom(b"ok")


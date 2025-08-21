import csv, os
from erlport.erlterms import Atom

_out = None
_writer = None

def _to_str(x):
    if isinstance(x, (bytes, bytearray, Atom)):
        return bytes(x).decode("utf-8", "ignore")
    if isinstance(x, list) and all(isinstance(i, int) for i in x):  # charlist
        return bytes(x).decode("utf-8", "ignore")
    return x if isinstance(x, str) else str(x)

def _norm_key(k):
    # Convert dict keys (Atom/bytes/charlist/str) to plain str
    if isinstance(k, (bytes, bytearray, Atom)):
        return bytes(k).decode("utf-8", "ignore")
    if isinstance(k, list) and all(isinstance(i, int) for i in k):
        return bytes(k).decode("utf-8", "ignore")
    return k if isinstance(k, str) else str(k)

def _norm_counts(d):
    return {_norm_key(k): v for k, v in d.items()} if isinstance(d, dict) else {}

def init(out_dir):
    global _out, _writer                      # <--- needed so assigns hit module vars
    out_dir = _to_str(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "throughput.csv")
    new = not os.path.exists(path)
    _out = open(path, "a", newline="", encoding="utf-8")
    _writer = csv.writer(_out)
    if new:
        _writer.writerow(["win_start_ms","messages","bytes"])

def apply(lat_ms, size_b, counts):
    global _out, _writer
    c = _norm_counts(counts)

    # win_start_ms is injected by window_engine; fall back to 0 if missing
    try:
        win_ms = int(c.get("win_start_ms", 0))
    except Exception:
        win_ms = 0

    msgs = int(c.get("recv", 0)) if "recv" in c else 0
    byts = sum(size_b) if isinstance(size_b, list) else 0

    if _writer:
        _writer.writerow([win_ms, msgs, byts])
        _out.flush()

    return {"msgs": msgs, "bytes": byts}

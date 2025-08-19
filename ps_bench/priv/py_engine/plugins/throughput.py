# priv/plugins/throughput.py
import csv, os
_out = None
_writer = None

def init(out_dir):
    global _out, _writer
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "throughput.csv")
    new = not os.path.exists(path)
    _out = open(path, "a", newline="", encoding="utf-8")
    _writer = csv.writer(_out)
    if new:
        _writer.writerow(["win_start_ms","messages","bytes"])

def apply(lat_ms, size_b, counts):
    msgs = counts.get(b"recv", 0) if isinstance(counts, dict) else 0
    byts = sum(size_b) if isinstance(size_b, list) else 0
    if _writer:
        _writer.writerow([counts.get(b"win_start_ms", 0), msgs, byts])
        _out.flush()
    # still return summary so Erlang gets it
    return {"msgs": msgs, "bytes": byts}

def _pct(lst, p):
    if not lst: return 0.0
    k = max(0, min(len(lst)-1, int(round((p/100.0)*(len(lst)-1)))))
    return float(sorted(lst)[k])

def apply(lat_ms, size_b, counts):
    if not lat_ms:
        return {'p50_ms':0.0,'p95_ms':0.0,'p99_ms':0.0,'mean_ms':0.0,'max_ms':0.0}
    n = len(lat_ms)
    return {
        'p50_ms': _pct(lat_ms, 50),
        'p95_ms': _pct(lat_ms, 95),
        'p99_ms': _pct(lat_ms, 99),
        'mean_ms': sum(lat_ms)/float(n),
        'max_ms': max(lat_ms)
    }

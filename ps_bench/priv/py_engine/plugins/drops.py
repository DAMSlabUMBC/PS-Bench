def apply(lat_ms, size_b, counts):
    drops = int(counts.get('drops', 0))
    recv = int(counts.get('recv', 0))
    rate = (drops / max(1, (drops + recv))) if (drops + recv) else 0.0
    return {'drops': drops, 'loss_rate': rate, 'recv': recv}

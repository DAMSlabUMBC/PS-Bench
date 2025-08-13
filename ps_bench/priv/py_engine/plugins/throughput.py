def apply(lat_ms, size_b, counts):
    recv = int(counts.get('recv', 0))
    bytes_total = sum(size_b) if size_b else 0
    return {'tput_msgs_s': recv, 'tput_bytes_s': bytes_total}

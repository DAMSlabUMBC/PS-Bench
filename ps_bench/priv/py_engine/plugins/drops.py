# Calculate drop metrics from the counts
def apply(lat_ms, size_b, counts):
    recv = counts.get('recv', 0)
    drops = counts.get('drops', 0)
    
    if recv + drops > 0:
        drop_rate = (drops / (recv + drops)) * 100
    else:
        drop_rate = 0.0
    
    return {
        'drops': drops,
        'drop_rate_pct': drop_rate
    }
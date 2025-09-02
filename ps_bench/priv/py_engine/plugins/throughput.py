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

def calc_for_runner(recv_events, pub_events, connect_events, disconnect_events, this_node_name, all_nodes_list):
    
    # Calculate this runner's throughput - Throughput = Sum(messages_recieved) / (total_time_connected)
    min_timestamp = -1
    max_timestamp = -1
    msgs_recv_count = 0
    
    pw_min_timestamp_dict = dict()
    pw_max_timestamp_dict = dict()
    pw_recv_message_total_dict = dict()
    
    for event in recv_events:
        recv_node_name = event[0]
        pub_node_name = event[2]
        recv_timestamp = event[6]
    
        if recv_node_name == this_node_name:
            
            # Calculate overall throughput
            msgs_recv_count += 1
            
            if min_timestamp == -1 or recv_timestamp < min_timestamp:
                min_timestamp = recv_timestamp
                
            if max_timestamp == -1 or recv_timestamp > max_timestamp:
                max_timestamp = recv_timestamp
                
            # Calculate pairwise throughput 
            if pub_node_name not in pw_min_timestamp_dict or recv_timestamp < pw_min_timestamp_dict[pub_node_name]:
                pw_min_timestamp_dict[pub_node_name] = recv_timestamp
                
            if pub_node_name not in pw_max_timestamp_dict or recv_timestamp > pw_max_timestamp_dict[pub_node_name]:
                pw_max_timestamp_dict[pub_node_name] = recv_timestamp

            if pub_node_name not in pw_recv_message_total_dict:
                pw_recv_message_total_dict[pub_node_name] = 0
            pw_recv_message_total_dict[pub_node_name] += 1
            
    total_time_ns = max_timestamp - min_timestamp
    total_time_s = total_time_ns / 1000000000.0
    throughput_s = msgs_recv_count / total_time_s
    
    pw_throughput_dict = dict()
    for node in pw_recv_message_total_dict:
        total_time_from_node_ns = pw_max_timestamp_dict[node] - pw_min_timestamp_dict[node]
        total_time_from_node_s = total_time_from_node_ns / 1000000000.0
        pw_throughput_dict[node] = pw_recv_message_total_dict[node] / total_time_from_node_s
    
    # # TODO: Need to actually store
    print(f"THROUGHPUT NODEWISE: {throughput_s} msg/s")
    print(f"THROUGHPUT PAIRWISE: {pw_throughput_dict}")
    
    return
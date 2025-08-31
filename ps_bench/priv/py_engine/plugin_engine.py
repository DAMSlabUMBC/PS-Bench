# priv/py_engine/window_engine.py
from erlport.erlterms import Atom
from erlport import erlang
from importlib import import_module
import os, sys

# Make sure we can import the plugins package at priv/plugins
_THIS_DIR = os.path.dirname(__file__)
_PRIV_DIR = os.path.dirname(_THIS_DIR)
if _PRIV_DIR not in sys.path:
    sys.path.append(_PRIV_DIR)

PLUGINS = []

def _to_str(x):
    # Normalize Erlang terms (Atom, binary, charlist) to a Python str
    if isinstance(x, (bytes, bytearray, Atom)):
        return bytes(x).decode("utf-8", "ignore")
    if isinstance(x, list) and all(isinstance(i, int) for i in x):  # Erlang charlist
        return bytes(x).decode("utf-8", "ignore")
    return x if isinstance(x, str) else str(x)

def _atom(name: str):
    return Atom(name.encode())

# Format after conversion: (RecvNodeName, SubscriberName, PublisherName, Topic, SeqId, PubTimestamp, RecvTimestamp, PayloadSize)
def _convert_recv_events_from_erlang(events):
    
    ret_list = list()
    
    recv_node_key = _atom("recv_node")
    sub_name_key = _atom("subscriber")
    seq_key = _atom("seq")
    size_key = _atom("bytes")
    pub_name_key = _atom("publisher")
    pub_ns_key = _atom("t_pub_ns")
    recv_ns_key = _atom("t_recv_ns")
    topic_key = _atom("topic")
    
    # Decompose events into tuples
    for event in events:
        
        # First element just the timestamp for sorting in erlang
        # Second element is the actual map
        data_map = event[1]
        recv_node = _to_str(data_map[recv_node_key])
        sub_name = _to_str(data_map[sub_name_key])
        seq = data_map[seq_key]
        size = data_map[size_key]
        pub_name = _to_str(data_map[pub_name_key])
        pub_ns = data_map[pub_ns_key]
        recv_ns = data_map[recv_ns_key]
        topic = _to_str(data_map[topic_key])
        
        event_tuple = (recv_node, sub_name, pub_name, topic, seq, pub_ns, recv_ns, size)
        ret_list.append(event_tuple)
        
    return ret_list

# Format after conversion: (NodeName, ClientName, Topic, SeqId)
def _convert_pub_events_from_erlang(events):
    
    ret_list = list()
    
    for event in events:
        source_node = _to_str(event[1])
        source_client = _to_str(event[2])
        topic = _to_str(event[3])
        seq = event[4]
        
        event_tuple = (source_node, source_client, topic, seq)
        ret_list.append(event_tuple)

    return ret_list

# Format after conversion: (ClientName, Timestamp)
def _convert_connect_events_from_erlang(events):
    
    ret_list = list()
    
    client_name_key = _atom("client")
    time_key = _atom("time")
    
    # Decompose events into tuples
    for event in events:
        
        # First element just the timestamp for sorting in erlang
        # Second element is the actual map
        data_map = event[1]
        client_name = _to_str(data_map[client_name_key])
        time = _to_str(data_map[time_key])
        
        event_tuple = (client_name, time)
        ret_list.append(event_tuple)
        
    return ret_list

# Format after conversion: (ClientName, DisconnectType, Timestamp)
def _convert_disconnect_events_from_erlang(events):
    
    ret_list = list()
    
    client_name_key = _atom("client")
    time_key = _atom("time")
    type_key = _atom("type")
    
    # Decompose events into tuples
    for event in events:
        
        # First element just the timestamp for sorting in erlang
        # Second element is the actual map
        data_map = event[1]
        client_name = _to_str(data_map[client_name_key])
        time = data_map[time_key]
        type = _to_str(data_map[type_key])
        
        event_tuple = (client_name, type, time)
        ret_list.append(event_tuple)
        
    return ret_list

def init(plugins, out_dir):
    global PLUGINS, OUT_DIR
    
    # Ensure OUT_DIR is a real str (not bytes/charlist)
    OUT_DIR = _to_str(out_dir)
    
    # Ensure plugin names are str as well
    plugins = [_to_str(p) for p in plugins]
    PLUGINS = [import_module(f"plugins.{p}") for p in plugins]
    
    for p in PLUGINS:
        init_fn = getattr(p, "init", None)
        if callable(init_fn):
            init_fn(OUT_DIR)
            
    return _atom("ok")

def calc_runner_metrics(recv_events, pub_events, connect_events, disconnect_events, this_node_name_erlang, all_nodes_list_erlang):
    global PLUGINS

    log_message = f'Converting recv events to python format'
    erlang.call(_atom("ps_bench_utils"), _atom("log_message"), [log_message])
    recv_events= erlang.call(_atom("ps_bench_store"), _atom("fetch_recv_events"), [])
    # recv_event_list = _convert_recv_events_from_erlang(recv_events)
    print(recv_events)
    
    log_message = f'Converting pub events to python format'
    erlang.call(_atom("ps_bench_utils"), _atom("log_message"), [log_message])
    pub_events= erlang.call(_atom("ps_bench_store"), _atom("fetch_publish_events"), [])
    # pub_event_list = _convert_pub_events_from_erlang(pub_events)
    
    log_message = f'Converting connect events to python format'
    erlang.call(_atom("ps_bench_utils"), _atom("log_message"), [log_message])
    connect_events= erlang.call(_atom("ps_bench_store"), _atom("fetch_publish_events"), [])
    # connect_event_list = _convert_connect_events_from_erlang(connect_events)
    
    log_message = f'Converting disconnect events to python format'
    erlang.call(_atom("ps_bench_utils"), _atom("log_message"), [log_message])
    disconnect_events= erlang.call(_atom("ps_bench_store"), _atom("fetch_disconnect_events"), [])
    # disconnect_event_list = _convert_disconnect_events_from_erlang(disconnect_events)
    
    this_node_name = _to_str(this_node_name_erlang)
    all_nodes_list = [_to_str(x) for x in all_nodes_list_erlang]
        
    for p in PLUGINS:
        calc_fn = getattr(p, "calc_for_runner", None)
        plugin_name = getattr(p, "__name__", "")
        simple_name = plugin_name.replace("plugins.", "")
        if callable(calc_fn):
            log_message = f'Calculating runner metrics with python plugin: {simple_name}'
            erlang.call(_atom("ps_bench_utils"), _atom("log_message"), [log_message])
            # calc_fn(recv_event_list, pub_event_list, connect_event_list, disconnect_event_list, this_node_name, all_nodes_list)
        else:
            log_message = f'No runner calculation method was defined for python plugin: {simple_name}'
            erlang.call(_atom("ps_bench_utils"), _atom("log_message"), [log_message])  
            
    return _atom("ok")
    


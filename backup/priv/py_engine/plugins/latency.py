def calc_for_runner(recv_events, pub_events, connect_events, disconnect_events, this_node_name, all_nodes_list):
    
    # We only care about recieved messages here
    # Calculate this runner's overall latency - Latency = Sum(RecvTimestamp - PubTimestamp) / num_messages_recv
    # and pairwise latency - For each runner j which sent messages recieved by this runner i, latency_ij is the latency calculated on ONLY messages sent by j and recieved by i
    latency_total = 0
    recv_message_total = 0
    pw_latency_total_dict = dict()
    pw_recv_message_total_dict = dict()
    
    for event in recv_events:
        
        # Recv Events format after conversion: (RecvNodeName, SubscriberName, PublisherName, Topic, SeqId, PubTimestamp, RecvTimestamp, PayloadSize)
        recv_node_name = event[0]
        pub_node_name = event[2]
        pub_timestamp = event[5]
        recv_timestamp = event[6]
        
        event_latency = abs(recv_timestamp - pub_timestamp)
        
        if recv_node_name == this_node_name:
            
            # Calculate total latency
            latency_total += (recv_timestamp - pub_timestamp)
            recv_message_total += 1
            
            # Calculate pairwise latency
            if pub_node_name not in pw_recv_message_total_dict:
                pw_latency_total_dict[pub_node_name] = 0.0
            pw_latency_total_dict[pub_node_name] += event_latency
            
            if pub_node_name not in pw_recv_message_total_dict:
                pw_recv_message_total_dict[pub_node_name] = 0
            pw_recv_message_total_dict[pub_node_name] += 1
            
    overall_latency_ns = (latency_total / recv_message_total)
    overall_latency_ms = overall_latency_ns / 1000000.0
    
    pw_latency_dict = dict()
    for node in pw_latency_total_dict:
        avg_latency_to_node_ns = (pw_latency_total_dict[node] / pw_recv_message_total_dict[node])
        avg_latency_to_node_ms = avg_latency_to_node_ns / 1000000.0
        pw_latency_dict[node] = avg_latency_to_node_ms
    
    # TODO: Need to actually store
    print(f"OVERALL: {overall_latency_ms}ms")
    print(f"PAIRWISE: {pw_latency_dict}")
    
    return
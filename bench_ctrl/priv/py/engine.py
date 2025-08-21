import sys, importlib, pickle, struct, traceback


registry = {}          

# A registry to hold the loaded modules and their functions.
def send(term):
    data = pickle.dumps(term, protocol=4)
    sys.stdout.buffer.write(struct.pack("!I", len(data)) + data)
    sys.stdout.flush()

# Function to receive messages from Erlang
def recv():
    hdr = sys.stdin.buffer.read(4)
    if not hdr:
        raise EOFError
    size, = struct.unpack("!I", hdr)
    return pickle.loads(sys.stdin.buffer.read(size))

while True:
    try:
        msg = recv()
    except EOFError:
        break

    try:
        if msg[0] == b'load':                       
            mod = importlib.import_module(msg[1].decode())
            registry[msg[1]] = getattr(mod, msg[1].decode())
        elif msg[0] == b'exec':                     
            _, ref, name, args = msg
            res = registry[name](*args)
            send((b'py', ref, res))                 
    except Exception as e:
        traceback.print_exc()
        send((b'py', msg[1], {'error': str(e)}))

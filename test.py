

def get_len(msg):
    return int(msg[10:13])

msg = '#:05343:S:085:S:N:20171107 125952:ALGOGROUP:1209604:16:32:43.5::NITE:E::MU:S:M:-1:0:*#:05343:A:051:ALGOGROUP:20000000.00:20000000.00:P:'
l = get_len(msg)


print msg[:l]
msg = msg[l:]
print msg

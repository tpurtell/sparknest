import json, sys
j = json.load(sys.stdin)["jobs"][0]["read"]
c = j["clat_ns"]
print(f"{j['iops']:.0f} reads/s  mean {c['mean']/1e6:.2f} ms  p99 {c['percentile']['99.000000']/1e6:.2f} ms")

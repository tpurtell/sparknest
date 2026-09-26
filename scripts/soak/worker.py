#!/usr/bin/env python3
"""Soak worker: runs on one host against its sparknest mount.

Writes files whose names carry the SHA-256 of their contents (some streamed,
some written at shuffled offsets from several threads, like hf_xet), deletes
its own old files, and reads random files written by any host, verifying
every byte against the name. A mismatch is corruption; an I/O error while a
daemon is down is expected and counted separately.
"""
import hashlib, json, os, random, sys, threading, time

mnt, host, seconds, out = sys.argv[1], sys.argv[2], float(sys.argv[3]), sys.argv[4]
root = os.path.join(mnt, "soak")
mine = os.path.join(root, host)
stats = dict(writes=0, write_bytes=0, write_errors=0, reads=0, read_bytes=0,
             read_errors=0, corrupt=0, deletes=0, delete_errors=0)
errors = {}
corrupt = []
rng = random.Random()

def note(kind, e):
    k = f"{kind}: {type(e).__name__}: {getattr(e, 'strerror', None) or e}"
    errors[k] = errors.get(k, 0) + 1

def size():
    r = rng.random()
    if r < 0.5: return rng.randint(1, 256 << 10)
    if r < 0.9: return rng.randint(256 << 10, 8 << 20)
    return rng.randint(8 << 20, 64 << 20)

def write_one(i):
    n = size()
    data = os.urandom(n)
    sha = hashlib.sha256(data).hexdigest()
    tmp = os.path.join(mine, f".{i}.tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        if n > (1 << 20) and rng.random() < 0.4:
            # xet-style: fixed chunks at shuffled offsets from 4 threads.
            chunk = 1 << 20
            offs = list(range(0, n, chunk)); rng.shuffle(offs)
            errs = []
            def w(part):
                try:
                    for o in part: os.pwrite(fd, data[o:o + chunk], o)
                except OSError as e: errs.append(e)
            ts = [threading.Thread(target=w, args=(offs[k::4],)) for k in range(4)]
            [t.start() for t in ts]; [t.join() for t in ts]
            if errs: raise errs[0]
        else:
            v = memoryview(data)
            while v:
                v = v[os.write(fd, v[:4 << 20]):]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(tmp, os.path.join(mine, f"{i:06d}-{sha[:32]}"))
    return n

def read_one():
    hosts = [h for h in os.listdir(root) if not h.startswith(".")]
    if not hosts: return
    d = os.path.join(root, rng.choice(hosts))
    names = [x for x in os.listdir(d) if not x.startswith(".")]
    if not names: return
    name = rng.choice(names)
    h = hashlib.sha256(); n = 0
    with open(os.path.join(d, name), "rb") as f:
        while True:
            b = f.read(4 << 20)
            if not b: break
            h.update(b); n += len(b)
    if h.hexdigest()[:32] != name.split("-", 1)[1]:
        stats["corrupt"] += 1
        corrupt.append(f"{d}/{name} ({n} bytes)")
    stats["reads"] += 1; stats["read_bytes"] += n

deadline = time.time() + seconds
i = 0
while time.time() < deadline:
    try:
        os.makedirs(mine, exist_ok=True)
        r = rng.random()
        if r < 0.45:
            i += 1
            stats["write_bytes"] += write_one(i); stats["writes"] += 1
        elif r < 0.55:
            own = sorted(x for x in os.listdir(mine) if not x.startswith("."))
            if len(own) > 150:
                os.unlink(os.path.join(mine, own[0])); stats["deletes"] += 1
        else:
            read_one()
    except FileNotFoundError as e:
        note("race", e)  # a file deleted between listing and opening
    except OSError as e:
        key = "write" if r < 0.45 else ("delete" if r < 0.55 else "read")
        stats[key + "_errors"] += 1; note(key, e)
        time.sleep(0.5)
    time.sleep(rng.random() * 0.2)
json.dump(dict(host=host, stats=stats, errors=errors, corrupt=corrupt), open(out, "w"), indent=1)

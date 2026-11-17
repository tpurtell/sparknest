"""sparknest-hf-fetch without the network: huggingface_hub and tqdm are stubbed."""
import importlib.machinery
import importlib.util
import io
import json
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent


class FakeTqdm:
    """Enough of tqdm for the driver's reporting subclass."""

    def __init__(self, *args, total=None, disable=False, file=None, **kwargs):
        self.total = total
        self.n = 0
        self.disable = disable

    def update(self, n=1):
        if not self.disable:
            self.n += n


def load_driver():
    tqdm_auto = types.ModuleType("tqdm.auto")
    tqdm_auto.tqdm = FakeTqdm
    sys.modules["tqdm"] = types.ModuleType("tqdm")
    sys.modules["tqdm.auto"] = tqdm_auto
    loader = importlib.machinery.SourceFileLoader("hf_fetch", str(HERE / "sparknest-hf-fetch"))
    spec = importlib.util.spec_from_loader("hf_fetch", loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


class FetchTest(unittest.TestCase):
    def run_fetch(self, download, names, direct=lambda args, name, report: False):
        hub = types.ModuleType("huggingface_hub")
        hub.hf_hub_download = download
        out = io.StringIO()
        args = types.SimpleNamespace(
            repo="org/m", repo_type="model", revision="a" * 40, cache_dir="/hub"
        )
        with mock.patch.dict(sys.modules, {"huggingface_hub": hub}), \
                mock.patch.object(self.driver, "direct", direct), \
                mock.patch.object(sys, "stdin", io.StringIO("".join(n + "\n" for n in names))), \
                mock.patch.object(sys, "stdout", out):
            self.driver.fetch(args)
        return [json.loads(l) for l in out.getvalue().splitlines()]

    def setUp(self):
        self.driver = load_driver()

    def test_reports_bytes_then_outcome_per_file_and_goes_on_after_errors(self):
        def download(repo, name, repo_type, revision, cache_dir, tqdm_class):
            self.assertEqual((repo, revision, cache_dir), ("org/m", "a" * 40, "/hub"))
            if name == "missing.bin":
                raise FileNotFoundError("404")
            # hf turns its bars off with disable=True: still reported.
            bar = tqdm_class(total=100, disable=True)
            bar._last = -1.0  # report the next update at once
            bar.update(60)

        got = self.run_fetch(download, ["a.bin", "missing.bin", "b.bin"])
        self.assertEqual(got[0], {"file": "a.bin", "done": 60, "total": 100})
        self.assertEqual(got[1], {"file": "a.bin", "ok": True})
        self.assertEqual(got[2]["file"], "missing.bin")
        self.assertIn("FileNotFoundError", got[2]["error"])
        self.assertEqual(got[-1], {"file": "b.bin", "ok": True})

    def test_direct_files_skip_hf_and_a_failed_direct_falls_back(self):
        via_hf = []

        def download(repo, name, **kw):
            via_hf.append(name)

        def direct(args, name, report):
            if name == "big.safetensors":
                report(5, 10)
                return True
            if name == "flaky.safetensors":
                raise OSError("sha256 mismatch")
            return False

        got = self.run_fetch(download, ["big.safetensors", "flaky.safetensors", "config.json"],
                             direct=direct)
        self.assertEqual(got[0], {"file": "big.safetensors", "done": 5, "total": 10})
        self.assertEqual(got[1], {"file": "big.safetensors", "ok": True})
        self.assertIn("using hf", got[2]["warning"])
        self.assertEqual(via_hf, ["flaky.safetensors", "config.json"])
        self.assertEqual(got[-1], {"file": "config.json", "ok": True})


class RangedTest(unittest.TestCase):
    """ranged_download against an in-memory server."""

    def setUp(self):
        self.driver = load_driver()
        self.data = bytes((i * 7 + i // 251) % 256 for i in range(3 << 20))

    def run_ranged(self, serve, **kw):
        out = bytearray(len(self.data))
        lock = threading.Lock()

        def write(off, b):
            with lock:
                out[off:off + len(b)] = b

        opts = dict(connections=4, piece=256 << 10, tick=0.02, window=0.3,
                    floor=1, slow_share=0.25, max_misses=8)
        opts.update(kw)
        self.driver.ranged_download(len(self.data), serve, write, **opts)
        return bytes(out)

    def chunks(self, s, e, delay=0.0, step=16 << 10, stop=None):
        closed = threading.Event()

        def gen():
            for a in range(s, e, step):
                if closed.is_set() or (stop is not None and a >= stop):
                    return
                if delay:
                    time.sleep(delay)
                yield self.data[a:min(a + step, e)]

        return gen(), closed.set

    def test_fetches_every_byte(self):
        self.assertEqual(self.run_ranged(lambda s, e: self.chunks(s, e)), self.data)

    def test_a_trickling_connection_is_replaced(self):
        # The first connection opened for the second piece trickles forever;
        # the download must not wait for it.
        calls = []
        lock = threading.Lock()

        def serve(s, e):
            with lock:
                calls.append(s)
                slow = s == 256 << 10 and calls.count(s) == 1
            return self.chunks(s, e, delay=0.2 if slow else 0.0, step=1024 if slow else 16 << 10)

        t0 = time.monotonic()
        got = self.run_ranged(serve)
        self.assertEqual(got, self.data)
        self.assertLess(time.monotonic() - t0, 10)
        # Its range was asked for again from where it stopped.
        self.assertTrue(any(c > 256 << 10 and c < 512 << 10 for c in calls), calls)

    def test_a_cut_connection_resumes_where_it_stopped(self):
        cut = {"done": False}
        lock = threading.Lock()

        def serve(s, e):
            with lock:
                first = not cut["done"] and s == 0
                cut["done"] = True
            return self.chunks(s, e, stop=(s + 100_000) if first else None)

        self.assertEqual(self.run_ranged(serve), self.data)

    def test_errors_give_up_after_repeated_empty_attempts(self):
        def serve(s, e):
            raise ConnectionError("refused")

        with self.assertRaises(OSError):
            self.run_ranged(serve, max_misses=3)

    def test_spans_merge(self):
        sp = self.driver.Spans()
        sp.add(10, 20)
        sp.add(0, 5)
        self.assertEqual(sp.prefix(), 5)
        sp.add(5, 10)
        self.assertEqual(sp.prefix(), 20)
        self.assertEqual(sp.total(), 20)


if __name__ == "__main__":
    unittest.main()

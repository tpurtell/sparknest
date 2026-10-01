"""sparknest-hf-fetch without the network: huggingface_hub and tqdm are stubbed."""
import importlib.machinery
import importlib.util
import io
import json
import sys
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
    def run_fetch(self, download, names):
        hub = types.ModuleType("huggingface_hub")
        hub.hf_hub_download = download
        out = io.StringIO()
        args = types.SimpleNamespace(
            repo="org/m", repo_type="model", revision="a" * 40, cache_dir="/hub"
        )
        with mock.patch.dict(sys.modules, {"huggingface_hub": hub}), \
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


if __name__ == "__main__":
    unittest.main()

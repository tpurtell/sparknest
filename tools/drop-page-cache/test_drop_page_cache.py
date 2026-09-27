"""Unprivileged checks: these tests never install a helper or drop caches."""
import os
from pathlib import Path
import subprocess
import shlex
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / 'sparknest-drop-page-cache'


class CacheDropUtility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.helper = Path(cls.directory.name) / 'helper'
        subprocess.run(['cc', '-std=c11', '-Wall', '-Wextra', '-Werror',
                        str(HERE / 'drop-page-cache.c'),
                        '-o', str(cls.helper)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_help_needs_no_privilege(self):
        for command in ([str(self.helper), '--help'], [str(SCRIPT), '--help']):
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_helper_rejects_other_actions(self):
        for args in (['3'], ['/tmp/control'], ['--help', '3'], ['--install']):
            result = subprocess.run([str(self.helper), *args], capture_output=True)
            self.assertEqual(result.returncode, 64)

    def test_install_transaction_sets_mode_and_replaces_inode(self):
        # Run the actual transaction in a private directory with our ownership.
        # No root-owned paths, privileged execution, or cache drops are involved.
        script = SCRIPT.read_text()
        start = script.index('    # Install a new inode')
        end = script.index('\n    exit 0', start)
        transaction = script[start:end]
        uid, gid = os.getuid(), os.getgid()
        with tempfile.TemporaryDirectory() as directory:
            # Reproduce an install that leaves the setuid bit cleared, as seen
            # on the release host. The explicit final chmod must restore it.
            installer = Path(directory) / 'install-with-cleared-setuid'
            installer.write_text(
                '#!/usr/bin/env bash\nset -euo pipefail\n'
                '/usr/bin/install "$@"\n/usr/bin/chmod u-s -- "${@: -1}"\n')
            installer.chmod(0o700)
            destination = Path(directory) / 'drop-page-cache'
            destination.write_text('old binary')
            old_fd = os.open(destination, os.O_RDONLY)
            try:
                transaction = transaction.replace(
                    '/usr/local/libexec/sparknest', directory)
                transaction = transaction.replace('-o root', f'-o {uid}')
                transaction = transaction.replace('/usr/bin/install', shlex.quote(str(installer)))
                transaction = transaction.replace('"0:$3:4750"', f'"{uid}:$3:4750"')
                prelude = (
                    'set -euo pipefail\n'
                    f'helper={shlex.quote(str(destination))}\n'
                    'check_helper() {\n'
                    ' [[ -f "$helper" && ! -L "$helper" && -x "$helper" ]] &&\n'
                    f' [[ $(stat -c "%u:%a" -- "$helper") == {uid}:4750 ]]\n'
                    '}\n')
                result = subprocess.run(
                    ['bash', '-c', prelude + transaction, 'test',
                     '--install-helper', str(self.helper), str(gid)],
                    capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                installed = destination.stat()
                self.assertEqual(installed.st_mode & 0o7777, 0o4750)
                self.assertEqual((installed.st_uid, installed.st_gid), (uid, gid))
                self.assertNotEqual(installed.st_ino, os.fstat(old_fd).st_ino)
                self.assertEqual(destination.read_bytes(), self.helper.read_bytes())
                self.assertFalse(list(Path(directory).glob('.drop-page-cache.*')))
            finally:
                os.close(old_fd)

    @unittest.skipIf(os.geteuid() == 0, 'No cache-drop execution under root in tests')
    def test_uninstalled_helper_cannot_flush(self):
        result = subprocess.run([str(self.helper)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 77)
        self.assertIn('setuid installation required', result.stderr)

    @unittest.skipIf(os.geteuid() == 0, 'No privileged installation in tests')
    def test_install_entry_rejects_unprivileged_caller(self):
        result = subprocess.run([str(SCRIPT), '--install-helper', str(self.helper), '0'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn('Invalid privileged installation', result.stderr)


if __name__ == '__main__':
    unittest.main()

"""Exercise the registration shell with a mock runner and a real pseudo-terminal."""

import errno
import os
from pathlib import Path
import pty
import re
import select
import subprocess
import tempfile
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "register-runner.sh"
PROMPT = b"Paste the GitHub runner registration token and press Enter."
TOKEN = b"dummy-registration-token"


class RunnerInputTests(unittest.TestCase):
    def setUp(self):
        fixture = tempfile.TemporaryDirectory()
        self.addCleanup(fixture.cleanup)
        self.cwd = Path(fixture.name)
        self.env = dict(os.environ, PATH=f"{self.cwd}:{os.environ['PATH']}")
        self.stub(
            "sudo",
            '[[ "$1 $2 $3" == "-u runner --" && "$*" != *dummy-registration-token* ]] || exit 70\n'
            'shift 3\nexec "$@"\n',
        )
        self.stub(
            "config.sh",
            '[[ "$ACTIONS_RUNNER_INPUT_TOKEN" == dummy-registration-token ]] || exit 71\n'
            '[[ "$*" == "--url https://github.com/test/lab --name lab-app --labels lab-app --unattended" ]] || exit 72\n'
            'echo mock-configured\n',
        )
        # Use the exact nested sudo command from the SSM payload.
        self.command = re.search(
            r"^  sudo -u runner -- bash -c '.*?^  ' bash [^\n]+",
            SCRIPT.read_text(),
            flags=re.M | re.S,
        ).group()
        self.command = "repo=test/lab\nlabel=lab-app\n" + self.command

    def stub(self, name, body):
        path = self.cwd / name
        path.write_text("#!/usr/bin/env bash\n" + body)
        path.chmod(0o700)

    def run_in_terminal(self, command, reply):
        pid, terminal = pty.fork()
        if pid == 0:
            os.chdir(self.cwd)
            os.execvpe("bash", ["bash", "-c", command], self.env)
        output = bytearray()
        replied = False
        deadline = time.monotonic() + 5
        try:
            while time.monotonic() < deadline:
                if not select.select([terminal], [], [], 0.1)[0]:
                    continue
                try:
                    chunk = os.read(terminal, 4096)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    break  # Linux PTY end of stream.
                if not chunk:
                    break
                output.extend(chunk)
                if PROMPT in output and not replied:
                    os.write(terminal, reply)
                    replied = True
            else:
                self.fail("Mock registration timed out")
        finally:
            os.close(terminal)
            _, status = os.waitpid(pid, 0)
        return os.waitstatus_to_exitcode(status), bytes(output)

    def test_piped_and_heredoc_stdin_use_controlling_terminal(self):
        for stdin in ("pipe", "heredoc"):
            with self.subTest(stdin=stdin):
                if stdin == "pipe":
                    command = "printf 'script input\\n' | (\n" + self.command + "\n)"
                else:
                    command = "(\n" + self.command + "\n) <<'STDIN'\nscript input\nSTDIN"
                status, output = self.run_in_terminal(command, TOKEN + b"\n")
                self.assertEqual(status, 0, output)
                self.assertIn(b"mock-configured", output)
                self.assertNotIn(TOKEN, output)
                self.assertEqual(sorted(os.listdir(self.cwd)), ["config.sh", "sudo"])

    def test_eof_and_empty_input(self):
        for reply, message in (
            (b"\x04", b"Token input ended before a complete line was entered."),
            (b"\n", b"The registration token was empty."),
        ):
            with self.subTest(reply=reply):
                status, output = self.run_in_terminal(self.command, reply)
                self.assertEqual(status, 1, output)
                self.assertIn(message, output)
                self.assertNotIn(b"mock-configured", output)

    def test_no_controlling_terminal(self):
        result = subprocess.run(
            ["bash", "-c", self.command],
            input=b"script input\n",
            capture_output=True,
            start_new_session=True,
            cwd=self.cwd,
            env=self.env,
            timeout=5,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"needs a controlling terminal", result.stderr)
        self.assertNotIn(PROMPT, result.stdout)


if __name__ == "__main__":
    unittest.main()

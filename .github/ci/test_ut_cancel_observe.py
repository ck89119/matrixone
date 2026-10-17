#!/usr/bin/env python3
"""诊断机制的确定性测试；这些测试不是原取消故障的修复证据。"""
import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import select
import signal
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

SCRIPT = Path(__file__).with_name("ut-cancel-observe.py")
spec = importlib.util.spec_from_file_location("ut_cancel_observe", SCRIPT)
diag = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diag)


class ObserverTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.case = self.root / (diag.TESTS[1] + "Fixture") / "001"
        self.case.mkdir(parents=True)
        self.checkpoint = self.case / "ut-report" / "ut-checkpoint.log"
        self.checkpoint.parent.mkdir()
        self.reader = diag.BashReader()

    def child(self, command=None, **kwargs):
        command = command or ["bash", "-c", 'trap "exit 143" TERM; printf "ready\\n"; IFS= read -r _']
        env = dict(os.environ, CASE_DIR=str(self.case))
        process = subprocess.Popen(command, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, **kwargs)

        def cleanup():
            if kwargs.get("start_new_session"):
                # supervisor 先退出也不能遗留同一测试 session 的 child。
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
        self.addCleanup(cleanup)
        return process

    def ready(self, process):
        self.assertTrue(select.select([process.stdout], [], [], 5)[0], "missing child readiness")
        return process.stdout.readline().decode().strip()

    def test_real_bash_identity_generation_and_neighbor(self):
        process = self.child()
        self.assertEqual(self.ready(process), "ready")
        state = self.reader.snapshot(process.pid, self.case, process.pid)
        self.assertEqual(state["trap_15"], "exit 143")
        self.assertEqual(state["pending_15"], 0)
        self.assertIsNone(self.reader.snapshot(process.pid, str(self.case) + "/neighbor", process.pid))
        self.assertIsNone(self.reader.snapshot(process.pid, self.case, process.pid, state["start"] + 1))
        neighbor = self.child()
        self.ready(neighbor)
        self.assertIsNone(self.reader.snapshot(process.pid, self.case, neighbor.pid))
        self.assertEqual(self.reader.snapshot(process.pid, self.case, os.getpid())["start"], state["start"])
        process.kill()
        process.wait(timeout=5)
        self.assertIsNone(self.reader.snapshot(process.pid, self.case, os.getpid()))

    def test_non_bash_executable_is_not_read(self):
        process = self.child([sys.executable, "-c", 'print("ready", flush=True); input()'])
        self.ready(process)
        self.assertIsNone(self.reader.snapshot(process.pid, self.case, process.pid))

    def test_generation_change_discards_snapshot(self):
        process = self.child()
        self.ready(process)
        original = diag.read_proc
        stat_reads = 0

        def changing(directory, name, limit):
            nonlocal stat_reads
            data = original(directory, name, limit)
            if name == "stat":
                stat_reads += 1
                if stat_reads == 2:
                    prefix, suffix = data.rsplit(b") ", 1)
                    fields = suffix.split()
                    fields[19] = str(int(fields[19]) + 1).encode()
                    return prefix + b") " + b" ".join(fields)
            return data
        with patch.object(diag, "read_proc", side_effect=changing):
            self.assertIsNone(self.reader.snapshot(process.pid, self.case, process.pid))
        self.assertEqual(stat_reads, 2)

    def test_memory_permission_and_mapping_errors_are_explicit(self):
        process = self.child()
        self.ready(process)
        original = diag.os.open

        def deny_memory(path, *args, **kwargs):
            if path == "mem":
                raise PermissionError(errno.EACCES, "denied")
            return original(path, *args, **kwargs)
        with patch.object(diag.os, "open", side_effect=deny_memory):
            with self.assertRaises(PermissionError):
                self.reader.snapshot(process.pid, self.case, process.pid)
        original_read = diag.read_proc
        with patch.object(diag, "read_proc", side_effect=lambda fd, name, limit: b"" if name == "maps" else original_read(fd, name, limit)):
            with self.assertRaisesRegex(diag.DiagnosticError, "mapping"):
                self.reader.snapshot(process.pid, self.case, process.pid)
        with patch.object(diag.os, "pread", return_value=b""):
            with self.assertRaisesRegex(diag.DiagnosticError, "short"):
                self.reader.snapshot(process.pid, self.case, process.pid)

    def test_exit_during_read_is_not_a_live_owner_permission_failure(self):
        for phase in ("mem", "maps"):
            with self.subTest(phase=phase):
                process = self.child()
                self.ready(process)
                original_open, original_read = diag.os.open, diag.read_proc

                def open_memory(path, *args, **kwargs):
                    if phase == "mem" and path == "mem":
                        process.kill()
                        process.wait(timeout=5)
                        raise PermissionError(errno.EACCES, "exited while opening mem")
                    return original_open(path, *args, **kwargs)

                def read_maps(fd, name, limit):
                    if phase == "maps" and name == "maps":
                        process.kill()
                        process.wait(timeout=5)
                        return b""
                    return original_read(fd, name, limit)
                with patch.object(diag.os, "open", side_effect=open_memory), patch.object(diag, "read_proc", side_effect=read_maps):
                    self.assertIsNone(self.reader.snapshot(process.pid, self.case, os.getpid()))

    def test_pf_exiting_precedes_zombie_and_rejects_stale_read(self):
        process = self.child()
        self.ready(process)
        path = f"/proc/{process.pid}/stat"
        prefix, suffix = Path(path).read_bytes().rsplit(b") ", 1)
        fields = suffix.split()
        fields[0] = b"R"
        fields[6] = str(int(fields[6]) | 0x4).encode()
        retiring = prefix + b") " + b" ".join(fields)
        real_open = open

        def retiring_stat(name, *args, **kwargs):
            return io.BytesIO(retiring) if name == path else real_open(name, *args, **kwargs)
        with patch.object(self.reader, "_snapshot", side_effect=PermissionError("exit_mm")), patch("builtins.open", side_effect=retiring_stat):
            self.assertIsNone(self.reader.snapshot(process.pid, self.case, process.pid))

    def test_elf_bounds_encoding_and_required_symbols(self):
        data = Path(diag.shutil.which("bash")).read_bytes()
        self.assertEqual(set(diag.elf_symbols(data)[1]), set(diag.SYMBOLS))
        corruptions = [b"", b"x" + data[1:], data[:18] + b"\x00\x00" + data[20:],
                       data.replace(b"pending_traps\0", b"pending_trapx\0")]
        damaged = bytearray(data)
        struct.pack_into("<Q", damaged, 40, len(data))
        corruptions.append(damaged)
        for broken in corruptions:
            with self.subTest(kind=bytes(broken[:20])):
                with self.assertRaises(diag.DiagnosticError):
                    diag.elf_symbols(broken)
        with patch.object(diag, "MAX_FILE", 64):
            with self.assertRaisesRegex(diag.DiagnosticError, "bound"):
                diag.BashReader()
        with patch.object(diag.shutil, "which", return_value=None):
            with self.assertRaisesRegex(diag.DiagnosticError, "not found"):
                diag.BashReader()

    def test_proc_read_limit_closes_descriptor(self):
        (self.root / "field").write_bytes(b"12345")
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, directory)
        count = len(os.listdir("/proc/self/fd"))
        with self.assertRaises(diag.DiagnosticError):
            diag.read_proc(directory, "field", 4)
        self.assertEqual(len(os.listdir("/proc/self/fd")), count)

    def test_checkpoint_selection_tail_and_nonregular_rejection(self):
        self.checkpoint.write_text("x" * 65536 + "\npid=1 event=start\npid=2 event=cancel status=143 detail=current_pid=3 plan_pid=4\n")
        self.assertEqual(diag.cancelled_owners(self.checkpoint), {2, 3, 4})
        self.checkpoint.unlink()
        os.mkfifo(self.checkpoint)
        with self.assertRaises(diag.DiagnosticError):
            diag.cancelled_owners(self.checkpoint)
        self.checkpoint.unlink()
        self.checkpoint.symlink_to(self.root / "field")
        with self.assertRaises(OSError):
            diag.cancelled_owners(self.checkpoint)

    def test_gone_owner_does_not_hide_remaining_owner_and_limits(self):
        self.checkpoint.write_text("pid=100 event=cancel detail=current_pid=101\n")
        reader = Mock()
        reader.snapshot.side_effect = [FileNotFoundError(errno.ENOENT, "gone"), {"start": 8, "pid": 101}]
        log = io.StringIO()
        observer = diag.Observer(reader, 1, diag.TESTS[1], self.root, log)
        with patch.object(diag, "cancelled_owners", return_value=[100, 101]):
            observer.scan()
        self.assertEqual(json.loads(log.getvalue())["pid"], 101)
        self.assertEqual(observer.samples, 1)
        reader.snapshot.side_effect = None
        reader.snapshot.return_value = {"start": 8, "pid": 101}
        with patch.object(diag, "cancelled_owners", return_value=[101]), patch.object(diag, "MAX_CASES", 1):
            observer.scan()  # 已登记代际不因容量已满而被拒绝。
        with patch.object(diag, "MAX_SAMPLES", 2):
            with self.assertRaises(diag.DiagnosticError):
                observer.scan()
        with patch.object(diag, "MAX_CASES", 0):
            with self.assertRaises(diag.DiagnosticError):
                observer.scan()
        reader.snapshot.side_effect = PermissionError(errno.EACCES, "denied")
        with self.assertRaises(PermissionError):
            observer.scan()
        reader.snapshot.side_effect = None
        reader.snapshot.return_value = None
        observer.scan()
        self.assertEqual(observer.samples, 2)

    def test_actual_checkpoint_consumer(self):
        process = self.child()
        self.ready(process)
        self.checkpoint.write_text(f"pid={process.pid} event=cancel detail=plan_pid={process.pid}\n")
        log = io.StringIO()
        observer = diag.Observer(self.reader, process.pid, diag.TESTS[1], self.root, log)
        observer.scan()
        self.assertEqual(json.loads(log.getvalue())["trap_15"], "exit 143")
        self.assertEqual(observer.samples, 1)

    def run_observed(self, command, **kwargs):
        result = diag.observe(command, self.root, diag.TESTS[1], self.root, **kwargs)
        status = json.loads((self.root / "observer-status.json").read_text())
        return result, status

    def test_real_command_exit_and_initialization_failure(self):
        for code in (0, 7):
            with self.subTest(code=code):
                result, status = self.run_observed([sys.executable, "-c", f"raise SystemExit({code})"])
                self.assertEqual((result, status["command_exit"], status["observer_exit"]), (code, code, 0))
                with patch.object(diag, "BashReader", side_effect=diag.DiagnosticError("missing symbols")):
                    result, status = self.run_observed([sys.executable, "-c", f"raise SystemExit({code})"])
                self.assertEqual((result, status["command_exit"], status["observer_exit"]), (code or 125, code, 125))
        result, status = self.run_observed([str(self.root / "no-such-command")])
        self.assertEqual(result, 125)
        self.assertIsNone(status["command_exit"])

    def test_scan_failure_preserves_original_terminal_result(self):
        for code in (0, 7, -15):
            process = Mock(returncode=code)
            process.poll.side_effect = [None, code, code]
            observer = Mock(samples=0, discarded=0)
            observer.scan.side_effect = PermissionError("denied")
            stop = Mock()
            stop.is_set.return_value = False
            with patch.object(diag.subprocess, "Popen", return_value=process), patch.object(diag, "Observer", return_value=observer):
                result, status = self.run_observed(["unused"], stop=stop)
            expected = code if code >= 0 else 128 - code
            self.assertEqual(result, expected or 125)
            self.assertEqual(status["command_exit"], expected)
            self.assertEqual(status["observer_exit"], 125)
            process.terminate.assert_not_called()

    def test_stop_escalation_and_exit_races(self):
        process = Mock()
        process.poll.return_value = None
        process.terminate.side_effect = ProcessLookupError()
        process.wait.side_effect = [subprocess.TimeoutExpired("test", 1), 137]
        process.kill.side_effect = ProcessLookupError()
        diag.stop_command(process)
        process.kill.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)
        process = self.child(["bash", "-c", 'trap "" TERM; printf "ready\\n"; IFS= read -r _'])
        self.ready(process)
        diag.stop_command(process)
        self.assertEqual(process.returncode, -signal.SIGKILL)

    def test_cli_signal_stops_and_reaps_owned_command(self):
        process = self.child([sys.executable, str(SCRIPT), "--output", str(self.root), "--test", diag.TESTS[1], "--",
                              "bash", "-c", 'trap "exit 143" TERM; printf "%s\\n" "$$"; IFS= read -r _'],
                             start_new_session=True)
        child_pid = int(self.ready(process))
        process.terminate()
        self.assertEqual(process.wait(timeout=5), 143)
        with self.assertRaises(ProcessLookupError):
            os.kill(child_pid, 0)
        status = json.loads((self.root / "observer-status.json").read_text())
        self.assertEqual(status["observer_exit"], 125)
        self.assertTrue(status["interrupted"])


if __name__ == "__main__":
    unittest.main()

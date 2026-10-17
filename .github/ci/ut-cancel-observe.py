#!/usr/bin/env python3
"""临时取消诊断：作为测试命令的祖先进程，只读指定夹具的 Bash 信号状态。

不 attach、不暂停目标、不修改 trap，不把非原子采样当作信号投递证明。
命令本身仍由 workflow 的 timeout 和原夹具期限约束。
"""
import argparse
import contextlib
import errno
import glob
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import struct
import subprocess
import threading
import time

TESTS = (
    "TestEmbeddedPrebuiltOuterCancellationKillsResistantGroup",
    "TestHeavyPlanCancellationStopsWritersBeforeMerge",
)
SCALARS = ("running_trap", "trapped_signal_received", "wait_intr_flag", "interrupt_immediately")
SYMBOLS = (*SCALARS, "pending_traps", "trap_list")
MAX_FILE = 16 * 1024 * 1024
MAX_CASES = 1024
MAX_SAMPLES = 4096


class DiagnosticError(Exception):
    pass


def elf_symbols(data):
    """只解析系统 Bash 的有界 ELF64 little-endian 动态符号表。"""
    if len(data) < 64 or data[:6] != b"\x7fELF\x02\x01":
        raise DiagnosticError("unsupported Bash ELF encoding")
    elf_type, machine = struct.unpack_from("<HH", data, 16)
    if elf_type not in (2, 3) or machine not in (62, 183):
        raise DiagnosticError("unsupported Bash ELF type/architecture")
    offset = struct.unpack_from("<Q", data, 40)[0]
    size, count = struct.unpack_from("<HH", data, 58)
    if size != 64 or not 0 < count <= 1024 or offset + count * size > len(data):
        raise DiagnosticError("invalid Bash ELF sections")
    sections = [struct.unpack_from("<IIQQQQIIQQ", data, offset + i * size) for i in range(count)]

    def section_bytes(section):
        start, length = section[4:6]
        if start + length > len(data):
            raise DiagnosticError("invalid Bash ELF section extent")
        return data[start:start + length]

    found = {}
    for section in sections:
        if section[1] != 11:  # SHT_DYNSYM
            continue
        if section[9] != 24 or section[5] % 24 or section[5] // 24 > 32768 or section[6] >= count:
            raise DiagnosticError("invalid Bash dynamic symbol table")
        if sections[section[6]][1] != 3:
            raise DiagnosticError("invalid Bash symbol string table")
        strings = section_bytes(sections[section[6]])
        for name, _, _, defined, value, _ in struct.iter_unpack("<IBBHQQ", section_bytes(section)):
            key = strings[name:name + 128].split(b"\0", 1)[0]
            for wanted in SYMBOLS:
                if defined and value and key == wanted.encode():
                    found[wanted] = value
    if set(found) != set(SYMBOLS):
        raise DiagnosticError("missing Bash signal symbols")
    return elf_type, found


def read_proc(directory, name, limit):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    with os.fdopen(fd, "rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise DiagnosticError("proc field exceeds diagnostic bound: " + name)
    return data


def process_identity(data):
    fields = data.rsplit(b") ", 1)[1].split()
    return int(fields[1]), int(fields[19])  # PPID, start time


def descendant(pid, owner):
    for _ in range(64):
        if pid == owner:
            return True
        if pid <= 1:
            return False
        with open(f"/proc/{pid}/stat", "rb") as stream:
            pid, _ = process_identity(stream.read(4096))
    raise DiagnosticError("process ancestry exceeds diagnostic bound")


class BashReader:
    def __init__(self, executable=None):
        executable = executable or shutil.which("bash")
        if not executable:
            raise DiagnosticError("Bash executable not found")
        with open(executable, "rb") as stream:
            info = os.fstat(stream.fileno())
            data = stream.read(MAX_FILE + 1)
        if len(data) > MAX_FILE:
            raise DiagnosticError("Bash executable exceeds diagnostic bound")
        self.identity = info.st_dev, info.st_ino
        self.elf_type, self.symbols = elf_symbols(data)
        self.sha256 = hashlib.sha256(data).hexdigest()

    def snapshot(self, pid, case, owner, generation=None):
        try:
            return self._snapshot(pid, case, owner, generation)
        except (OSError, DiagnosticError) as failure:
            # 退出中的 Bash 可以先失去 maps/environ 访问权，再从 procfs 消失。
            # 只在确认死亡、旧代际或失去祖先关系时丢弃；存活 owner 的拒绝访问仍是错误。
            try:
                with open(f"/proc/{pid}/stat", "rb") as stream:
                    data = stream.read(4096)
                _, current = process_identity(data)
                fields = data.rsplit(b") ", 1)[1].split()
                # Linux PF_EXITING 在 exit_mm()/变成 zombie 之前发布。
                if fields[0] in (b"Z", b"X") or int(fields[6]) & 0x4:
                    return None
                if generation is not None and generation != current:
                    return None
                if not descendant(pid, owner):
                    return None
                failure.observer_process = {"pid": pid, "start": current, "state": fields[0].decode(), "flags": int(fields[6])}
            except OSError as error:
                if error.errno in (errno.ENOENT, errno.ESRCH):
                    return None
                raise
            raise

    def _snapshot(self, pid, case, owner, generation):
        with contextlib.ExitStack() as stack:
            directory = os.open(f"/proc/{pid}", os.O_RDONLY | os.O_DIRECTORY)
            stack.callback(os.close, directory)
            before = process_identity(read_proc(directory, "stat", 4096))
            if generation is not None and before[1] != generation:
                return None
            if not descendant(pid, owner):
                return None
            identity = b"CASE_DIR=" + os.fsencode(case)
            if identity not in read_proc(directory, "environ", 128 * 1024).split(b"\0"):
                return None
            executable = os.stat("exe", dir_fd=directory)
            if (executable.st_dev, executable.st_ino) != self.identity:
                return None
            base = None
            for line in read_proc(directory, "maps", 128 * 1024).splitlines():
                fields = line.split()
                major, minor = (int(part, 16) for part in fields[3].split(b":"))
                if int(fields[2], 16) == 0 and (os.makedev(major, minor), int(fields[4])) == self.identity:
                    base = int(fields[0].split(b"-")[0], 16) if self.elf_type == 3 else 0
                    break
            if base is None:
                raise DiagnosticError("Bash mapping not found")
            memory = os.open("mem", os.O_RDONLY, dir_fd=directory)
            stack.callback(os.close, memory)

            def scalar(name, index=0, fmt="<i"):
                size = struct.calcsize(fmt)
                value = os.pread(memory, size, base + self.symbols[name] + size * index)
                if len(value) != size:
                    raise DiagnosticError("short Bash memory read")
                return struct.unpack(fmt, value)[0]

            result = {"pid": pid, "start": before[1], "case": str(case)}
            result.update({name: scalar(name) for name in SCALARS})
            for sig in (2, 15, 17):
                result[f"pending_{sig}"] = scalar("pending_traps", sig)
                pointer = scalar("trap_list", sig, "<Q")
                if pointer in (0, 1, 2**64 - 1):
                    trap = hex(pointer)
                else:
                    trap = os.pread(memory, 256, pointer).split(b"\0", 1)[0].decode(errors="replace")
                result[f"trap_{sig}"] = trap
            after = process_identity(read_proc(directory, "stat", 4096))
            if after != before or identity not in read_proc(directory, "environ", 128 * 1024).split(b"\0"):
                return None
            return result


def cancelled_owners(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise DiagnosticError("checkpoint is not a regular file")
        stream.seek(max(0, info.st_size - 65536))
        text = stream.read(65536).decode(errors="replace")
    owners = set()
    for line in text.splitlines():
        if " event=cancel " in line:
            owners.update(int(pid) for pid in re.findall(r"(?:^| )(?:pid|current_pid|light_pid|engine_pid|plan_pid|prebuild_pid)=(\d+)", line.replace(" detail=", " ")))
    return owners


class Observer:
    def __init__(self, reader, owner, test, temp_root, log):
        self.reader, self.owner, self.log = reader, owner, log
        self.pattern = str(Path(temp_root) / (test + "*") / "*" / "ut-report" / "ut-checkpoint.log")
        self.generations = {}
        self.samples = 0
        self.discarded = 0

    def scan(self):
        for index, path in enumerate(glob.iglob(self.pattern)):
            if index >= MAX_CASES:
                raise DiagnosticError("fixture scan limit reached")
            case = Path(path).parents[1]
            try:
                owners = cancelled_owners(path)
            except FileNotFoundError:
                continue
            for pid in owners:
                key = str(case), pid
                try:
                    sample = self.reader.snapshot(pid, case, self.owner, self.generations.get(key))
                except OSError as error:
                    # 一个 owner 已退出不能使同一 checkpoint 的其他 owner 永久失去采样。
                    if error.errno in (errno.ENOENT, errno.ESRCH):
                        self.discarded += 1
                        continue
                    raise
                if sample is None:
                    self.discarded += 1
                    continue
                if self.samples >= MAX_SAMPLES or (key not in self.generations and len(self.generations) >= MAX_CASES):
                    raise DiagnosticError("Bash sample limit reached")
                self.generations[key] = sample["start"]
                self.log.write(json.dumps({"time": time.time(), **sample}) + "\n")
                self.log.flush()
                self.samples += 1


def stop_command(command):
    if command.poll() is None:
        try:
            command.terminate()
        except ProcessLookupError:
            pass
        try:
            command.wait(timeout=1)
        except subprocess.TimeoutExpired:
            try:
                command.kill()
            except ProcessLookupError:
                pass
            command.wait(timeout=1)


def observe(command, output, test, temp_root, stop=None):
    stop = stop or threading.Event()
    status = {"command_exit": None, "observer_exit": 0, "samples": 0}
    process = observer = None
    with (Path(output) / "bash-state.jsonl").open("w") as log:
        try:
            try:
                reader = BashReader()
                status["bash_sha256"] = reader.sha256
            except Exception as error:
                reader = None
                status.update(observer_exit=125, error=str(error))
            process = subprocess.Popen(command)
            if reader is not None:
                observer = Observer(reader, process.pid, test, temp_root, log)
            while process.poll() is None and not stop.is_set():
                if observer is not None and status["observer_exit"] == 0:
                    try:
                        observer.scan()
                    except Exception as error:
                        status.update(observer_exit=125, error=str(error), process=getattr(error, "observer_process", None))
                stop.wait(0.05)  # 只限制外部采样频率，不充当被测夹具的同步条件。
        except Exception as error:
            status.update(observer_exit=125, error=str(error))
        finally:
            if process is not None:
                try:
                    stop_command(process)
                except Exception as error:
                    status.update(observer_exit=125, cleanup_error=str(error))
                if process.returncode is not None:
                    status["command_exit"] = process.returncode if process.returncode >= 0 else 128 - process.returncode
            if observer is not None:
                status["samples"] = observer.samples
                status["discarded"] = observer.discarded
            if stop.is_set():
                status.update(observer_exit=125, interrupted=True)
            (Path(output) / "observer-status.json").write_text(json.dumps(status) + "\n")
    return status["command_exit"] or status["observer_exit"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--test", choices=TESTS, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("missing test command")
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    return observe(command, args.output, args.test, os.environ.get("TMPDIR", "/tmp"), stop)


if __name__ == "__main__":
    raise SystemExit(main())

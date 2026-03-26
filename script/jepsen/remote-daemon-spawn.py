#!/usr/bin/env python3

import os
import sys


def main() -> int:
    if len(sys.argv) < 3:
        print(
            "usage: remote-daemon-spawn.py LOG_PATH CMD [ARGS...]",
            file=sys.stderr,
        )
        return 2

    log_path = sys.argv[1]
    command = sys.argv[2:]
    log_dir = os.path.dirname(log_path)

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    pid = os.fork()
    if pid:
        print("started", flush=True)
        os._exit(0)

    os.setsid()

    pid = os.fork()
    if pid:
        os._exit(0)

    devnull_fd = os.open("/dev/null", os.O_RDONLY)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)

    os.dup2(devnull_fd, 0)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)

    if devnull_fd > 2:
        os.close(devnull_fd)
    if log_fd > 2:
        os.close(log_fd)

    for fd in range(3, 256):
        try:
            os.close(fd)
        except OSError:
            pass

    os.chdir("/")
    os.execvp(command[0], command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

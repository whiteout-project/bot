"""Shared, platform-aware process restart.

Used by both the startup updater (main.py) and the in-app Restart button
(cogs/bot_health.py) so there is a single source of truth for how the bot
relaunches itself. Stdlib-only and side-effect-free so it is safe to import
during main.py's early bootstrap and from a cog. Not a cog itself (no setup()).
"""
import os
import sys


def is_container() -> bool:
    """Check if running in a container (Docker, Kubernetes, Podman, LXC, systemd-nspawn)."""
    # Docker, Kubernetes, Podman - simple marker file checks
    marker_files = ["/.dockerenv", "/var/run/secrets/kubernetes.io", "/run/.containerenv"]
    if any(os.path.exists(path) for path in marker_files):
        return True

    # LXC - check init process environment
    try:
        with open("/proc/1/environ", "r") as f:
            if "container=lxc" in f.read():
                return True
    except (IOError, OSError):
        pass

    # Systemd-nspawn - check container type file
    try:
        with open("/run/systemd/container", "r") as f:
            if f.read().strip() == "systemd-nspawn":
                return True
    except (IOError, OSError):
        pass

    return False


def restart_process(allow_update: bool = False):
    """Restart the current bot process in a platform-aware way.

    - Container: clean exit; the orchestrator (Docker/k8s) relaunches it.
    - Windows host: print the run command and exit. Windows can't reliably
      relaunch itself (a spawned child races the parent shell for stdin).
    - Linux/Mac: os.execl() for in-place replacement.

    One-shot flags (--repair, --no-venv) are filtered from the relaunch args
    to avoid loops. --no-update is filtered only when allow_update is True, so
    the startup updater can install a pending release on relaunch.
    """
    drop = {"--repair", "--no-venv"}
    if allow_update:
        drop.add("--no-update")
    relaunch_args = [arg for arg in sys.argv if arg not in drop]

    if is_container():
        print("  Restarting bot...")
        sys.exit(0)

    if sys.platform == "win32":
        venv_python = os.path.join("bot_venv", "Scripts", "python.exe")
        python = venv_python if os.path.exists(venv_python) else "python"
        print()
        print("=" * 60)
        print("  Bot stopped. To restart, run:")
        print(f"    {python} {' '.join(relaunch_args)}")
        print("=" * 60)
        print()
        sys.exit(0)

    print("  Restarting bot...")
    os.execl(sys.executable, sys.executable, *relaunch_args)

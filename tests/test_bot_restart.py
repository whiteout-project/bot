"""restart_process relaunches in place on Linux hosts and in containers, so hosting panels never see the bot stop."""
import sys

import pytest

from cogs import bot_restart


class _Relaunched(Exception):
    pass


@pytest.fixture
def relaunch(monkeypatch):
    calls = []

    def fake_execl(path, *args):
        calls.append(args)
        raise _Relaunched

    monkeypatch.setattr(bot_restart.os, "execl", fake_execl)
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(sys, "argv", ["main.py", "--autoupdate", "--repair", "--no-update"])
    return calls


@pytest.mark.parametrize("container", [True, False])
def test_linux_and_containers_relaunch_in_place(monkeypatch, relaunch, container):
    monkeypatch.setattr(bot_restart, "is_container", lambda: container)
    with pytest.raises(_Relaunched):
        bot_restart.restart_process()
    assert relaunch[0][1:] == ("main.py", "--autoupdate", "--no-update")


def test_update_restart_drops_no_update(monkeypatch, relaunch):
    monkeypatch.setattr(bot_restart, "is_container", lambda: True)
    with pytest.raises(_Relaunched):
        bot_restart.restart_process(allow_update=True)
    assert relaunch[0][1:] == ("main.py", "--autoupdate")

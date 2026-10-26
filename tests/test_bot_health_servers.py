"""Maintenance -> Leave a Server: listing the bot's servers and leaving one (bot owner only)."""
import asyncio
import types

from cogs import bot_health


class _Guild:
    def __init__(self, gid, name, members):
        self.id, self.name, self.member_count = gid, name, members
        self.left = False

    async def leave(self):
        self.left = True


def _cog(guilds):
    by_id = {g.id: g for g in guilds}
    bot = types.SimpleNamespace(guilds=guilds, get_guild=by_id.get)
    return types.SimpleNamespace(bot=bot, logger=__import__("logging").getLogger("test"))


def _interaction(user_id):
    sent = []

    async def send_message(text, ephemeral=False):
        sent.append((text, ephemeral))

    return types.SimpleNamespace(user=types.SimpleNamespace(id=user_id),
                                 response=types.SimpleNamespace(send_message=send_message)), sent


def test_lists_every_server_and_offers_all_but_the_current_one(monkeypatch):
    monkeypatch.setattr(bot_health, "_alliances_per_server", lambda: {2: 3})
    guilds = [_Guild(1, "Zulu", 40), _Guild(2, "alpha", 1), _Guild(3, "Mike", 12)]

    async def run():
        view = bot_health.ServersView(_cog(guilds), current_guild_id=3)
        embed = await view.build_embed()
        select = next(c for c in view.children if hasattr(c, "options"))
        return embed, select

    embed, select = asyncio.run(run())
    text = embed.description
    assert text.index("alpha") < text.index("Mike") < text.index("Zulu")
    assert "**Mike** (this server)" in text
    assert "1 member, 3 alliances" in text and "40 members, 0 alliances" in text
    assert [o.value for o in select.options] == ["2", "1"]


def test_only_the_bot_owner_can_use_it(monkeypatch):
    monkeypatch.setattr(bot_health.PermissionManager, "is_owner", staticmethod(lambda uid: uid == 7))
    view = bot_health.ServersView(_cog([]), None)
    owner, owner_sent = _interaction(7)
    other, other_sent = _interaction(8)

    assert asyncio.run(view.interaction_check(owner)) is True and owner_sent == []
    assert asyncio.run(view.interaction_check(other)) is False
    assert other_sent and "bot owner" in other_sent[0][0] and other_sent[0][1] is True


def test_leave_makes_the_bot_leave_and_reports_it():
    guild = _Guild(5, "Wolves", 30)
    view = bot_health.ServersView(_cog([guild]), None)
    asyncio.run(view.leave(5))
    assert guild.left and "left **Wolves**" in view.last_result

    asyncio.run(view.leave(99))
    assert view.last_result == "The bot had already left that server."

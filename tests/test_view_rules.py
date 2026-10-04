"""Static checks for the menu rules: view timeouts, Back vs Main Menu, no text above embeds, Discord length limits.

Parses cogs/*.py only, so a broken view fails here before anyone opens Discord.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
COG_FILES = sorted((REPO / "cogs").glob("*.py"))

VIEW_BASES = {"View", "ui.View", "discord.ui.View"}
MENU_TIMEOUT = "menu_timeout()"  # bot-wide setting, pimp_my_bot.menu_timeout
CONFIRM_TIMEOUT = "confirm_timeout()"  # bot-wide setting, pimp_my_bot.confirm_timeout
ALLOWED_TIMEOUTS = {MENU_TIMEOUT, CONFIRM_TIMEOUT}
DISCORD_DEFAULT_TIMEOUT = 180
TIMEOUT_EXCEPTIONS = {
    "MainMenuView": "top-level settings hub never expires (CLAUDE.md)",
    "UploadWaitView": "stays open for the backup upload window",
    "ScheduleBoardPaginationView": "lives on the public schedule board message",
    "BearSessionView": "lives on the public bear session message",
}
CONTENT_EXCEPTIONS = {
    ("pimp_my_bot.py", "content"): "safe_edit_message passes content through",
    ("notification_editor.py", "mention_preview"): "preview shows the ping as it will be sent",
    ("notification_system.py", "mention_preview"): "preview shows the ping as it will be sent",
    ("notification_system.py", "content"): "embed editor help sits above the user's own embed preview",
}

TREES = {f: ast.parse(f.read_text(encoding="utf-8")) for f in COG_FILES}


def _class_index() -> dict[str, list[tuple[Path, ast.ClassDef]]]:
    index = {}
    for f, tree in TREES.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                index.setdefault(node.name, []).append((f, node))
    return index


CLASSES = _class_index()


def _repo_base(f: Path, cls: ast.ClassDef):
    for base in cls.bases:
        defs = CLASSES.get(ast.unparse(base).split(".")[-1], [])
        same_file = [d for d in defs if d[0] == f]
        if same_file or defs:
            return (same_file or defs)[0]
    return None


def _is_view(f: Path, cls: ast.ClassDef, depth: int = 0) -> bool:
    if any(ast.unparse(b) in VIEW_BASES for b in cls.bases):
        return True
    base = _repo_base(f, cls)
    return depth < 10 and base is not None and _is_view(*base, depth + 1)


VIEWS = [(f, cls) for defs in CLASSES.values() for f, cls in defs if _is_view(f, cls)]
VIEW_NAMES = {cls.name for _, cls in VIEWS}


def _walk_own(node: ast.AST):
    """ast.walk that does not descend into nested classes."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.ClassDef):
            continue
        yield child
        yield from _walk_own(child)


def _text(expr: ast.expr) -> str:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return expr.value
    if isinstance(expr, ast.JoinedStr):
        return "".join(_text(v) if isinstance(v, ast.Constant) else "{" + ast.unparse(v.value) + "}"
                       for v in expr.values)
    return ast.unparse(expr)


def _fixed_length(expr: ast.expr) -> int:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return len(expr.value)
    if isinstance(expr, ast.JoinedStr):
        return sum(len(v.value) for v in expr.values if isinstance(v, ast.Constant))
    return 0


def _kw(call: ast.Call, name: str):
    return next((k.value for k in call.keywords if k.arg == name), None)


def _is_none(expr) -> bool:
    return isinstance(expr, ast.Constant) and expr.value in (None, "")


def _calls(tree: ast.AST):
    return (n for n in ast.walk(tree) if isinstance(n, ast.Call))


# ── Timeouts: menu_timeout() menus, confirm_timeout() confirmations, None only for persistent or public views ──
def _module_constants(tree: ast.Module) -> dict[str, object]:
    consts = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    consts[target.id] = node.value.value
    return consts


CONSTANTS = {f: _module_constants(tree) for f, tree in TREES.items()}
CALLER_SUPPLIED = "caller"


def _init(cls: ast.ClassDef):
    return next((n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "__init__"), None)


def _param_defaults(func: ast.FunctionDef) -> tuple[dict, set]:
    args = func.args
    positional = args.posonlyargs + args.args
    names = [p.arg for p in positional]
    defaults = dict(zip(names[len(names) - len(args.defaults):], args.defaults))
    defaults.update({p.arg: d for p, d in zip(args.kwonlyargs, args.kw_defaults) if d is not None})
    return defaults, set(names) | {p.arg for p in args.kwonlyargs}


def _is_super_init(call: ast.Call) -> bool:
    func = call.func
    return (isinstance(func, ast.Attribute) and func.attr == "__init__"
            and (ast.unparse(func.value).startswith("super(") or ast.unparse(func.value) in VIEW_BASES))


def _resolve(f: Path, expr: ast.expr, defaults: dict, params: set):
    if isinstance(expr, ast.Constant):
        return expr.value
    if isinstance(expr, ast.Name) and expr.id in params:
        return _resolve(f, defaults[expr.id], {}, set()) if expr.id in defaults else CALLER_SUPPLIED
    if isinstance(expr, ast.Name) and expr.id in CONSTANTS[f]:
        return CONSTANTS[f][expr.id]
    return ast.unparse(expr)


def _timeouts(f: Path, cls: ast.ClassDef, depth: int = 0) -> list:
    base = _repo_base(f, cls)
    inherited = _timeouts(*base, depth + 1) if base and depth < 10 else [DISCORD_DEFAULT_TIMEOUT]
    init = _init(cls)
    super_calls = [c for c in _calls(init) if _is_super_init(c)] if init else []
    if not super_calls:
        return inherited
    defaults, params = _param_defaults(init)
    found = []
    for call in super_calls:
        timeout = _kw(call, "timeout")
        found += inherited if timeout is None else [_resolve(f, timeout, defaults, params)]
    return found


def _persistent_views() -> set[str]:
    return {m.group(1) for f in COG_FILES
            for m in re.finditer(r"add_view\(\s*(\w+)\(", f.read_text(encoding="utf-8"))}


def _allowed_timeouts(name: str, persistent: set[str]) -> set:
    if name in persistent:
        return {None}
    return {CONFIRM_TIMEOUT} if "Confirm" in name else ALLOWED_TIMEOUTS


def test_view_classes_use_standard_timeouts():
    persistent = _persistent_views()
    bad = []
    for f, cls in VIEWS:
        if cls.name in TIMEOUT_EXCEPTIONS:
            continue
        allowed = _allowed_timeouts(cls.name, persistent)
        for value in _timeouts(f, cls):
            if value != CALLER_SUPPLIED and value not in allowed:
                bad.append(f"{f.name}:{cls.lineno} {cls.name} timeout={value!r} (allowed {sorted(allowed, key=str)})")
    assert not bad, "views with a non-standard timeout:\n  " + "\n  ".join(sorted(set(bad)))


def _assigned_names(tree: ast.AST) -> dict[int, str]:
    return {id(n.value): ast.unparse(n.targets[0]) for n in ast.walk(tree)
            if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)}


def test_view_constructor_calls_use_standard_timeouts():
    persistent = _persistent_views()
    bad = []
    for f, tree in TREES.items():
        assigned = _assigned_names(tree)
        for call in _calls(tree):
            name = ast.unparse(call.func)
            timeout = _kw(call, "timeout")
            if name in VIEW_BASES:
                value = DISCORD_DEFAULT_TIMEOUT if timeout is None else _resolve(f, timeout, {}, set())
                is_confirm = "confirm" in assigned.get(id(call), "").lower()
                allowed = {CONFIRM_TIMEOUT} if is_confirm else ALLOWED_TIMEOUTS
            elif name in VIEW_NAMES and name not in TIMEOUT_EXCEPTIONS and timeout is not None:
                value = _resolve(f, timeout, {}, set())
                allowed = _allowed_timeouts(name, persistent)
            else:
                continue
            if value not in allowed and not isinstance(value, str):
                bad.append(f"{f.name}:{call.lineno} {name}(timeout={value!r}) (allowed {sorted(allowed, key=str)})")
    assert not bad, "view constructions with a non-standard timeout:\n  " + "\n  ".join(sorted(bad))


# ── Navigation: a view offers Back or Main Menu, never both ──
def _button_texts(cls: ast.ClassDef) -> list[str]:
    texts = []
    for node in _walk_own(cls):
        if isinstance(node, ast.Call):
            parts = [_text(k.value) for k in node.keywords if k.arg in ("label", "emoji")]
            if parts:
                texts.append(" ".join(parts))
    return texts


def _is_home(text: str) -> bool:
    return "Main Menu" in text or "homeIcon" in text


def _is_back(text: str) -> bool:
    plain = re.sub(r"\{[^}]*\}", "", text).strip()
    return plain == "Back" or (plain.startswith("Back to ") and not _is_home(plain))


def test_no_view_has_both_back_and_main_menu():
    bad = []
    for f, cls in VIEWS:
        texts = _button_texts(cls)
        if any(_is_home(t) for t in texts) and any(_is_back(t) and not _is_home(t) for t in texts):
            bad.append(f"{f.name}:{cls.lineno} {cls.name}")
    assert not bad, "views with both a Back and a Main Menu button:\n  " + "\n  ".join(bad)


# ── Embeds: all text goes in the embed, never in content above it ──
def _content(call: ast.Call):
    positional = ast.unparse(call.func).split(".")[-1] in ("send", "send_message") and call.args
    return call.args[0] if positional else _kw(call, "content")


def test_no_message_text_above_embeds():
    bad = []
    for f, tree in TREES.items():
        for call in _calls(tree):
            content = _content(call)
            embed = _kw(call, "embed") or _kw(call, "embeds")
            if content is None or embed is None or _is_none(content) or _is_none(embed):
                continue
            if (f.name, ast.unparse(content)) not in CONTENT_EXCEPTIONS:
                bad.append(f"{f.name}:{call.lineno} content={ast.unparse(content)[:60]}")
    assert not bad, "calls sending text above an embed:\n  " + "\n  ".join(bad)


# ── Discord limits on fixed text (a too-long label fails the whole message) ──
def _limits_for(call: ast.Call) -> dict[str, int]:
    name = ast.unparse(call.func).split(".")[-1]
    if name == "SelectOption":
        return {"label": 100, "description": 100, "value": 100}
    if name == "TextInput":
        return {"label": 45, "placeholder": 100}
    return {"label": 80, "placeholder": 150, "custom_id": 100}


def test_component_text_within_discord_limits():
    bad = []
    for f, tree in TREES.items():
        for call in _calls(tree):
            for field, limit in _limits_for(call).items():
                value = _kw(call, field)
                if value is not None and _fixed_length(value) > limit:
                    bad.append(f"{f.name}:{call.lineno} {field} longer than {limit}: {_text(value)[:50]!r}")
            options = _kw(call, "options")
            if isinstance(options, ast.List) and len(options.elts) > 25:
                bad.append(f"{f.name}:{call.lineno} {len(options.elts)} select options (max 25)")
    assert not bad, "component text over Discord's limits:\n  " + "\n  ".join(bad)

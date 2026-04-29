#!/usr/bin/env python3
"""ioBroker plugin for SJBA.

Uses the ioBroker simple-api adapter (typically port 8087) to control and query
smart home devices.

Devices are resolved from ioBroker's enum (rooms/functions) assignments — only
devices the user has explicitly configured in ioBroker are shown in lists and
get priority in fuzzy search.

API format (ioBroker simple-api adapter)
-----------------------------------------
  GET  /objects?pattern=*&type=state    all state objects
  GET  /objects?pattern=enum.*&type=enum  enum (room/function) assignments
  GET  /get/{id}                        read state value
  GET  /set/{id}?value={v}              write state value
  GET  /toggle/{id}                     toggle boolean state
  GET  /getBulk/{id1},{id2},...          read multiple states

Environment variables
---------------------
IOBROKER_URL          Base URL of ioBroker simple-api (default: http://iobroker.eb.localdomain:8087)
IOBROKER_CACHE_FILE   Path to local device cache JSON (default: /tmp/iobroker_cache.json)
IOBROKER_CACHE_TTL    Cache TTL in seconds (default: 3600)
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL   = os.environ.get("IOBROKER_URL", "http://iobroker.eb.localdomain:8087").rstrip("/")
CACHE_FILE = Path(os.environ.get("IOBROKER_CACHE_FILE", "/tmp/iobroker_cache.json"))
CACHE_TTL  = int(os.environ.get("IOBROKER_CACHE_TTL", "3600"))


# ---------------------------------------------------------------------------
# REST helpers
# ---------------------------------------------------------------------------

def _get(path: str) -> dict | list | None:
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {path}: {body[:200]}") from exc
    except Exception as exc:
        raise RuntimeError(f"Request failed {path}: {exc}") from exc


def _get_state(obj_id: str) -> dict | None:
    return _get(f"/get/{urllib.parse.quote(obj_id, safe='.')}")


def _set_state(obj_id: str, value) -> None:
    eid = urllib.parse.quote(obj_id, safe=".")
    # Use lowercase true/false so ioBroker parses booleans correctly
    if isinstance(value, bool):
        raw = "true" if value else "false"
    else:
        raw = str(value)
    ev = urllib.parse.quote(raw, safe="")
    _get(f"/set/{eid}?value={ev}")


def _toggle_state(obj_id: str) -> dict | None:
    return _get(f"/toggle/{urllib.parse.quote(obj_id, safe='.')}")


def _get_bulk(ids: list[str]) -> list[dict]:
    if not ids:
        return []
    encoded = ",".join(urllib.parse.quote(i, safe=".") for i in ids)
    result = _get(f"/getBulk/{encoded}")
    if isinstance(result, list):
        return result
    return []


# ---------------------------------------------------------------------------
# Cache building
# ---------------------------------------------------------------------------

_SKIP_PREFIXES = (
    "system.", "admin.", "script.", "log.", "alias.0.automatically_generated",
    "javascript.", "alexa2.", "heatingcontrol.", "radar2.",
)

# Roles that are useful for direct control or primary sensor reading
_CONTROL_ROLES = {
    "switch", "switch.light", "switch.power", "switch.lock",
    "level", "level.brightness", "level.dimmer", "level.color.temperature",
    "level.temperature",
}
_SENSOR_ROLES = {
    "value.temperature", "value.humidity", "value.power", "value.energy",
    "value.window", "value.door", "value.blind", "value.curtain",
}
# State names to skip in list (too generic/internal)
_SKIP_STATE_NAMES = {
    "cloud connection", "cloud-verbindung", "cloud verbindung",
    "auto timer off", "auto timer on", "autotimeroff", "automatischer timer aus",
    "automatischer timer ein", "autotimeron", "reboot", "update firmware",
    "new firmware available",
}


def _fetch_enums() -> dict[str, dict]:
    """Return {device_prefix: {name, rooms, functions}} for enum-assigned devices."""
    try:
        raw = _get("/objects?pattern=enum.*&type=enum")
    except RuntimeError:
        return {}
    if not isinstance(raw, dict):
        return {}

    device_map: dict[str, dict] = {}
    for obj_id, obj in raw.items():
        common = obj.get("common", {})
        enum_name = common.get("name", obj_id)
        if isinstance(enum_name, dict):
            enum_name = enum_name.get("de") or enum_name.get("en") or obj_id
        members = common.get("members", [])
        is_room = obj_id.startswith("enum.rooms.")
        is_func = obj_id.startswith("enum.functions.")
        for member in members:
            if member not in device_map:
                device_map[member] = {"name": "", "rooms": [], "functions": []}
            if is_room:
                device_map[member]["rooms"].append(str(enum_name))
            elif is_func:
                device_map[member]["functions"].append(str(enum_name))
    return device_map


def _fetch_device_names(device_prefixes: list[str]) -> dict[str, str]:
    """Read .name state values for Shelly/similar devices to get friendly names."""
    name_ids = [f"{p}.name" for p in device_prefixes]
    try:
        results = _get_bulk(name_ids)
    except RuntimeError:
        return {}
    out = {}
    for item in results:
        item_id = item.get("id") or ""
        val = item.get("val")
        if item_id.endswith(".name") and isinstance(val, str) and val.strip():
            prefix = item_id[:-5]  # remove .name suffix
            out[prefix] = val.strip()
    return out


def _build_cache() -> list[dict]:
    """Fetch all state objects + enums, return enriched device list."""
    print("[iobroker] fetching objects...", file=sys.stderr)
    try:
        raw_states = _get("/objects?pattern=*&type=state")
    except RuntimeError as exc:
        raise RuntimeError(f"Could not fetch objects: {exc}") from exc
    if not isinstance(raw_states, dict):
        return []

    print("[iobroker] fetching enums...", file=sys.stderr)
    enum_map = _fetch_enums()

    # Fetch friendly names for enum members (shelly-style .name states)
    enum_prefixes = list(enum_map.keys())
    print(f"[iobroker] fetching device names for {len(enum_prefixes)} enum members...", file=sys.stderr)
    friendly_names = _fetch_device_names(enum_prefixes)

    # Enrich enum entries with friendly names
    for prefix, fname in friendly_names.items():
        if prefix in enum_map:
            enum_map[prefix]["name"] = fname

    devices = []
    for obj_id, obj in raw_states.items():
        if not isinstance(obj, dict):
            continue
        if any(obj_id.startswith(p) for p in _SKIP_PREFIXES):
            continue

        common = obj.get("common", {})
        name = common.get("name", "")
        if isinstance(name, dict):
            name = name.get("de") or name.get("en") or next(iter(name.values()), "")
        name = str(name).strip()
        if not name:
            continue

        role = common.get("role", "")
        unit = common.get("unit", "")
        writable = common.get("write", True)
        readable = common.get("read", True)
        type_ = common.get("type", "")

        # Resolve enum membership: a state's device prefix is obj_id up to last component
        # e.g. shelly.0.SHPLG-S#2229DD#1.Relay0.Switch → check shelly.0.SHPLG-S#2229DD#1.Relay0
        #      and shelly.0.SHPLG-S#2229DD#1 (parent device)
        enum_info = None
        parts = obj_id.split(".")
        for depth in range(len(parts) - 1, 0, -1):
            prefix = ".".join(parts[:depth])
            if prefix in enum_map:
                enum_info = enum_map[prefix]
                break

        rooms = enum_info["rooms"] if enum_info else []
        functions = enum_info["functions"] if enum_info else []
        device_name = enum_info["name"] if enum_info else ""

        # Build a display name: prefer device_name + state role/name for Shelly-like
        display_name = name
        if device_name and name.lower() in ("switch", "state", "brightness", "level",
                                             "schalter", "dimmer"):
            display_name = f"{device_name}"
        elif device_name:
            display_name = f"{device_name} {name}"

        is_control = role in _CONTROL_ROLES
        is_sensor = role in _SENSOR_ROLES
        in_enum = enum_info is not None

        devices.append({
            "id": obj_id,
            "name": name,
            "display_name": display_name,
            "device_name": device_name,
            "role": role,
            "unit": unit,
            "type": type_,
            "writable": writable,
            "readable": readable,
            "rooms": rooms,
            "functions": functions,
            "in_enum": in_enum,
            "is_control": is_control,
            "is_sensor": is_sensor,
        })

    return devices


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------

def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as fh:
                return json.load(fh)
        except Exception:
            pass
    return {"ts": 0, "devices": []}


def _save_cache(devices: list[dict]) -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, "w") as fh:
            json.dump({"ts": time.time(), "devices": devices}, fh)
    except Exception as exc:
        print(f"[iobroker] warning: could not save cache: {exc}", file=sys.stderr)


def _get_devices(force_reload: bool = False) -> list[dict]:
    cache = _load_cache()
    age = time.time() - cache.get("ts", 0)
    if force_reload or age > CACHE_TTL or not cache.get("devices"):
        devices = _build_cache()
        _save_cache(devices)
        return devices
    return cache["devices"]


# ---------------------------------------------------------------------------
# Fuzzy matching
# ---------------------------------------------------------------------------

def _score(query: str, device: dict) -> int:
    q = query.lower()
    obj_id = device["id"].lower()
    in_enum = device.get("in_enum", False)
    bonus = 20 if in_enum else 0  # prefer configured devices

    for field in ("display_name", "device_name", "name"):
        text = device.get(field, "").lower()
        if not text:
            continue
        if q == text:
            return 100 + bonus
        if q in text:
            score = 50 + (len(q) * 10 // max(len(text), 1)) + bonus
            return score

    # room name match
    for room in device.get("rooms", []):
        if q in room.lower():
            return 30 + bonus

    if q in obj_id:
        return 25 + bonus

    # word overlap — for multi-word queries ALL words must be found
    # include room/function names so "wohnzimmer licht" can match room+name
    q_words = q.split()
    if q_words:
        all_text = " ".join(
            device.get(f, "").lower()
            for f in ("display_name", "device_name", "name")
        )
        all_text += " " + " ".join(r.lower() for r in device.get("rooms", []))
        all_text += " " + " ".join(f.lower() for f in device.get("functions", []))
        all_words = set(all_text.split())
        matched = [w for w in q_words if w in all_words]
        # require full coverage for multi-word queries
        if len(q_words) == 1 and matched:
            return 10 + bonus
        if len(matched) == len(q_words):
            return 15 + bonus

    return 0


def _find_device(query: str, devices: list[dict]) -> dict | None:
    scored = [(d, _score(query, d)) for d in devices]
    scored.sort(key=lambda x: x[1], reverse=True)
    best_device, best_score = scored[0] if scored else (None, 0)
    return best_device if best_score >= 10 else None


def _find_device_with_reload(query: str) -> tuple[dict | None, list[dict]]:
    devices = _get_devices()
    device = _find_device(query, devices)
    if device is None:
        devices = _get_devices(force_reload=True)
        device = _find_device(query, devices)
    return device, devices


# ---------------------------------------------------------------------------
# Command parsing
# ---------------------------------------------------------------------------

def _parse_command(text: str) -> tuple[str, str, str | None]:
    words = text.strip().split()
    if words and words[0].lower() == "iobroker":
        words = words[1:]

    if not words:
        return ("help", "", None)

    verb = words[0].lower()

    if verb == "list":
        subject = " ".join(words[1:]).lower() if len(words) > 1 else ""
        return ("list", subject, None)
    if verb == "reload":
        return ("reload", "", None)
    if verb == "help":
        return ("help", "", None)
    if verb == "get" and len(words) >= 2:
        return ("get", " ".join(words[1:]), None)
    if verb == "set" and len(words) >= 3:
        return ("set", " ".join(words[1:-1]), words[-1])
    if verb == "turn" and len(words) >= 3:
        return ("set", " ".join(words[2:]), words[1].lower())
    if verb == "toggle" and len(words) >= 2:
        return ("toggle", " ".join(words[1:]), None)

    if len(words) >= 2:
        last = words[-1].lower()
        if last in ("on", "off", "toggle"):
            subject = " ".join(words[:-1])
            if last == "toggle":
                return ("toggle", subject, None)
            return ("set", subject, last)

    return ("get", " ".join(words), None)


def _coerce_value(value_str: str, device: dict):
    v = value_str.lower()
    if v in ("on", "true", "yes", "1", "ein", "an"):
        return True
    if v in ("off", "false", "no", "0", "aus"):
        return False
    try:
        return float(value_str) if "." in value_str else int(value_str)
    except ValueError:
        return value_str


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

HELP_TEXT = """\
ioBroker plugin commands:
  iobroker list [room]           — list configured devices (optionally filter by room)
  iobroker reload                — force reload device cache
  iobroker get <device>          — read current value
  iobroker set <device> <value>  — set value
  iobroker turn on/off <device>  — turn device on or off
  iobroker toggle <device>       — toggle boolean device
  iobroker <device> on/off       — shorthand"""


def _cmd_list(devices: list[dict], filter_str: str = "") -> str:
    # Prefer enum-configured, then control/sensor roles; skip noisy states
    listable = [
        d for d in devices
        if d.get("in_enum")
        and (d.get("is_control") or d.get("is_sensor"))
        and d.get("name", "").lower() not in _SKIP_STATE_NAMES
        and d.get("display_name", "").lower() not in _SKIP_STATE_NAMES
    ]

    if filter_str:
        listable = [
            d for d in listable
            if filter_str in " ".join(d.get("rooms", [])).lower()
            or filter_str in d.get("display_name", "").lower()
            or filter_str in d.get("device_name", "").lower()
        ]

    if not listable:
        fallback = [d for d in devices if d.get("is_control") or d.get("is_sensor")]
        if not fallback:
            return "No devices found. Try: iobroker reload"
        listable = fallback[:50]

    # Group by room, deduplicate by display_name per room
    by_room: dict[str, list[dict]] = {}
    for d in listable:
        room = d["rooms"][0] if d["rooms"] else "Sonstige"
        by_room.setdefault(room, []).append(d)

    lines = []
    shown = 0
    for room in sorted(by_room.keys()):
        room_entries = sorted(by_room[room], key=lambda x: x["display_name"].lower())
        seen_names: set[str] = set()
        room_lines = []
        for d in room_entries:
            key = d["display_name"].lower()
            if key in seen_names:
                continue
            seen_names.add(key)
            unit = f" [{d['unit']}]" if d["unit"] else ""
            rw = "rw" if d["writable"] else "r "
            room_lines.append(f"  [{rw}] {d['display_name']}{unit}")
        if room_lines:
            lines.append(f"\n{room}:")
            lines.extend(room_lines)
            shown += len(room_lines)
        if shown >= 60:
            break

    total_enum = len([d for d in devices if d.get("in_enum") and (d.get("is_control") or d.get("is_sensor"))])
    lines.insert(0, f"Configured devices ({total_enum} total{', filtered: ' + filter_str if filter_str else ''}):")
    return "\n".join(lines)


def _cmd_get(subject: str) -> str:
    device, _ = _find_device_with_reload(subject)
    if device is None:
        return f"Device not found: '{subject}'"
    try:
        state = _get_state(device["id"])
        if state is None:
            return f"{device['display_name']}: (no state)"
        val = state.get("val")
        unit = device["unit"]
        return f"{device['display_name']}: {_fmt_value(val)}{' ' + unit if unit else ''}"
    except RuntimeError as exc:
        return f"Error reading {device['display_name']}: {exc}"


def _fmt_value(value) -> str:
    if value is True:
        return "on"
    if value is False:
        return "off"
    return str(value)


def _cmd_set(subject: str, value_str: str) -> str:
    device, _ = _find_device_with_reload(subject)
    if device is None:
        return f"Device not found: '{subject}'"
    if not device["writable"]:
        return f"{device['display_name']} is read-only"
    value = _coerce_value(value_str, device)
    try:
        # Read current value first — avoid sending redundant writes to devices
        # that use toggle semantics (e.g. intex SPA) where every write = toggle.
        # Check both the value AND whether a write is already pending (ack=false).
        state = _get_state(device["id"])
        if state:
            current = state.get("val")
            ack = state.get("ack", True)
            if current == value:
                unit = f" {device['unit']}" if device["unit"] else ""
                return f"{device['display_name']} already {_fmt_value(value)}{unit}"
            if not ack and current != value:
                # A write with target value is already pending — don't double-send
                unit = f" {device['unit']}" if device["unit"] else ""
                return f"{device['display_name']} command already pending ({_fmt_value(value)}{unit})"
        _set_state(device["id"], value)
        unit = f" {device['unit']}" if device["unit"] else ""
        return f"{device['display_name']} set to {_fmt_value(value)}{unit}"
    except RuntimeError as exc:
        return f"Error setting {device['display_name']}: {exc}"


def _cmd_toggle(subject: str) -> str:
    device, _ = _find_device_with_reload(subject)
    if device is None:
        return f"Device not found: '{subject}'"
    if not device["writable"]:
        return f"{device['display_name']} is read-only"
    try:
        result = _toggle_state(device["id"])
        val = result.get("val") if isinstance(result, dict) else "?"
        return f"{device['display_name']} toggled → {val}"
    except RuntimeError as exc:
        return f"Error toggling {device['display_name']}: {exc}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    envelope = json.load(sys.stdin)
    text = (envelope.get("pipeline_text") or envelope.get("text", "")).strip()

    if not text:
        print(json.dumps({"continue": True}))
        return

    verb, subject, value_str = _parse_command(text)

    try:
        if verb == "help":
            reply = HELP_TEXT
        elif verb == "list":
            devices = _get_devices()
            reply = _cmd_list(devices, subject)
        elif verb == "reload":
            devices = _get_devices(force_reload=True)
            enum_count = len(set(d["id"].rsplit(".", 1)[0] for d in devices if d.get("in_enum")))
            reply = f"Device cache reloaded. {len(devices)} states, {enum_count} configured devices."
        elif verb == "get":
            reply = _cmd_get(subject)
        elif verb == "set":
            reply = _cmd_set(subject, value_str or "")
        elif verb == "toggle":
            reply = _cmd_toggle(subject)
        else:
            reply = HELP_TEXT
    except Exception as exc:
        print(f"[iobroker] unexpected error: {exc}", file=sys.stderr, flush=True)
        reply = f"ioBroker error: {exc}"

    print(json.dumps({
        "continue": False,
        "respond": {"text": reply, "target": "source"},
    }))


if __name__ == "__main__":
    main()

"""Collect and verify the registered Go/Rust HTTP route inventory."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable


METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
RUST_METHODS = {method.lower() for method in METHODS}
GO_ROUTE = re.compile(r'"(?P<method>[A-Z]+) (?P<path>[^"]+)"\s*:')
RUST_ROUTE = re.compile(
    r'\.route\(\s*"(?P<path>[^"]+)"\s*,\s*(?P<method>get|post|put|patch|delete|head|options)\s*\(',
    re.DOTALL,
)


def _pairs(items: Iterable[tuple[str, str]]) -> set[tuple[str, str]]:
    return {(method.upper(), path) for method, path in items}


def collect_routes(root: Path) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    go_source = (root / "backend/internal/httpapi/routes.go").read_text()
    go_routes = _pairs(
        (match.group("method"), match.group("path"))
        for match in GO_ROUTE.finditer(go_source)
        if match.group("method") in METHODS
    )

    rust_routes: set[tuple[str, str]] = set()
    for source in (root / "rust/src").rglob("*.rs"):
        text = source.read_text()
        rust_routes.update(
            (match.group("method").upper(), match.group("path"))
            for match in RUST_ROUTE.finditer(text)
            if match.group("method") in RUST_METHODS
        )
    return go_routes, rust_routes


def owner_for(path: str) -> str:
    if path.startswith("/api/v2/"):
        return "queue-realtime-v2"
    if "/admin/" in path or path.startswith("/api/v1/admin"):
        return "admin-control-plane"
    if path.startswith("/api/v1/session") or path.startswith("/api/v1/workspaces"):
        return "identity-workspaces"
    if path.startswith("/api/v1/dashboard"):
        return "dashboard"
    if path.startswith("/api/v1/realtime") or path == "/api/v1/socket":
        return "realtime"
    if path.startswith("/api/v1/events"):
        return "events-delivery"
    if path.startswith("/api/v1/functions"):
        return "functions"
    if path.startswith("/api/v1/routing"):
        return "routing"
    if path.startswith("/api/v1/apps"):
        return "apps"
    if path.startswith("/api/v1/jobs"):
        return "queue"
    return "public-surface"


def load_manifest(path: Path) -> dict:
    data = json.loads(path.read_text())
    if data.get("version") != 1 or not isinstance(data.get("routes"), list):
        raise ValueError("route parity manifest must be version 1 with a routes list")
    return data


def verify_manifest(
    manifest: dict,
    go_routes: set[tuple[str, str]],
    rust_routes: set[tuple[str, str]],
) -> list[str]:
    errors: list[str] = []
    entries = manifest.get("routes", [])
    keys: list[tuple[str, str]] = []
    for index, entry in enumerate(entries):
        method = entry.get("method")
        path = entry.get("path")
        if method not in METHODS or not isinstance(path, str) or not path:
            errors.append(f"routes[{index}] has invalid method/path")
            continue
        keys.append((method, path))
        if not entry.get("owner"):
            errors.append(f"{method} {path} has no owner")
        if entry.get("status") not in {"rust", "legacy-go", "compatibility"}:
            errors.append(f"{method} {path} has invalid status")
    if len(keys) != len(set(keys)):
        errors.append("manifest contains duplicate method/path entries")

    present = set(keys)
    for label, expected in (("Go", go_routes), ("Rust", rust_routes)):
        for method, path in sorted(expected - present):
            errors.append(f"missing {label} route: {method} {path}")
    return errors


def build_manifest(root: Path) -> dict:
    go_routes, rust_routes = collect_routes(root)
    entries = []
    for method, path in sorted(go_routes | rust_routes, key=lambda item: (item[1], item[0])):
        status = "rust" if (method, path) in rust_routes else "legacy-go"
        entries.append({"method": method, "path": path, "owner": owner_for(path), "status": status})
    return {
        "version": 1,
        "generated_from": [
            "backend/internal/httpapi/routes.go",
            "rust/src/**/*.rs",
        ],
        "routes": entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    destination = root / "contracts/route-parity.json"
    generated = build_manifest(root)
    if args.write:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(generated, indent=2) + "\n")
    manifest = load_manifest(destination)
    errors = verify_manifest(manifest, *collect_routes(root))
    if errors:
        print("\n".join(errors))
        return 1
    print(f"route parity OK: {len(manifest['routes'])} registered routes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

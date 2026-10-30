#!/usr/bin/env python3
"""Portable, deterministic verification for the public RelayHub Developer Kit."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def run(command, cwd, env=None):
    return subprocess.run(command, cwd=cwd, env=env, check=True, text=True, capture_output=True)


def verify_manifest(artifacts):
    manifest_path = artifacts / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    assert manifest['schema_version'] == 1
    assert manifest['product_version'] == 'v1'
    assert manifest['components'], 'public download manifest has no components'
    for component in manifest['components']:
        relative_url = component['relative_url']
        path = artifacts / relative_url.removeprefix('/downloads/') if relative_url.startswith('/downloads/') else artifacts.parent / 'skills' / relative_url.removeprefix('/skills/')
        assert path.is_file(), f"missing artifact: {component['relative_url']}"
        assert sha256(path) == component['sha256'], f"SHA256 mismatch: {component['name']}"
        assert component['build_command'] and component['test_command']
    return manifest


def verify_zip_boundaries(path):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        assert names == sorted(names), 'archive order is not deterministic'
        forbidden_parts = {'.git', 'node_modules', '__pycache__', '.env', 'Dockerfile', 'compose.yaml', 'deploy', 'server'}
        assert all(not any(part in forbidden_parts or part.startswith('.') for part in Path(name).parts) for name in names)
        joined = b'\n'.join(archive.read(name) for name in names if not name.endswith(('.json', '.lock', '.sum')))
        assert b'RELAYHUB_SIGNING_SECRET=' not in joined
        assert b'RELAYHUB_POSTGRES_PASSWORD=' not in joined
    return names


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--artifacts', type=Path, required=True)
    args = parser.parse_args()
    artifacts = args.artifacts.resolve()
    manifest = verify_manifest(artifacts)
    kit = artifacts / 'relayhub-developer-kit-v1.zip'
    assert kit.is_file()
    kit_names = verify_zip_boundaries(kit)
    assert any(name.endswith('package-lock.json') for name in kit_names)
    assert any(name.endswith('go.mod') for name in kit_names)
    assert any(name.endswith('LICENSE') for name in kit_names)

    with tempfile.TemporaryDirectory(prefix='relayhub kit fixture ') as temp:
        first = Path(temp) / 'first build'
        second = Path(temp) / 'second build'
        run(['sh', str(ROOT / 'backend/scripts/build-skill.sh'), '--kit-output', str(first)], ROOT)
        run(['sh', str(ROOT / 'backend/scripts/build-skill.sh'), '--kit-output', str(second)], ROOT)
        assert (first / 'relayhub-developer-kit-v1.zip').read_bytes() == (second / 'relayhub-developer-kit-v1.zip').read_bytes(), 'kit build is not reproducible'
        extracted = Path(temp) / 'kit with spaces'
        extracted.mkdir()
        with zipfile.ZipFile(kit) as archive:
            archive.extractall(extracted)
        kit_root = extracted / 'relayhub-developer-kit-v1'
        assert (kit_root / 'manifest.json').is_file()
        assert (kit_root / 'examples' / 'node' / 'package.json').is_file()
        assert 'v1' in (kit_root / 'examples' / 'node' / 'README.md').read_text()
        assert 'v1' in (kit_root / 'examples' / 'go' / 'README.md').read_text()

        node_sdk = kit_root / 'sdks' / 'typescript'
        run(['npm', 'ci', '--no-audit', '--no-fund'], node_sdk)
        run(['npm', 'run', 'build'], node_sdk)
        run(['npm', 'test'], node_sdk)
        run(['npm', 'pack', '--ignore-scripts'], node_sdk)
        go_sdk = kit_root / 'sdks' / 'go'
        go_env = dict(os.environ, GOWORK='off')
        run(['go', 'test', './...'], go_sdk, env=go_env)

        hostile = Path(temp) / 'private fixtures' / '.env'
        hostile.parent.mkdir()
        hostile.write_text('RELAYHUB_SIGNING_SECRET=not-for-public-output\n')
        assert '.env' not in '\n'.join(kit_names)

    tampered = artifacts / 'tampered-kit.zip'
    shutil.copyfile(kit, tampered)
    with tampered.open('ab') as handle:
        handle.write(b'tampered')
    assert sha256(tampered) != next(c['sha256'] for c in manifest['components'] if c['name'] == 'relayhub-developer-kit')
    tampered.unlink()

    print(f"PASS: Developer Kit ({manifest['product_version']}, {len(manifest['components'])} components)")


if __name__ == '__main__':
    main()

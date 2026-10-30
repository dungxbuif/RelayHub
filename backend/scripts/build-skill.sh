#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
cd "$repo_dir"
exec "${PYTHON:-python3}" - "$@" <<'PY'
import argparse
import hashlib
import io
import json
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

ROOT = Path.cwd()
STATIC = ROOT / 'web/docs/static'
SKILL = STATIC / 'skills/relayhub-integration'
KIT_VERSION = 'v1'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def deterministic_zip(entries):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_STORED) as archive:
        for name, data in sorted(entries.items()):
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
    return stream.getvalue()


def write_or_check(path, data, check):
    if check:
        assert path.is_file() and path.read_bytes() == data, f'{path} drift; rerun build-skill.sh'
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def collect_sdk(language):
    source = ROOT / 'sdks' / language
    assert source.is_dir(), f'missing SDK source: {source}'
    excluded = {'node_modules', 'dist', '.git', 'coverage', '.cache'}
    suffixes = {'.ts', '.go', '.json', '.mjs', '.md', '.mod', '.sum'}
    entries = {}
    for path in sorted(source.rglob('*')):
        relative = path.relative_to(source)
        if not path.is_file() or path.is_symlink() or any(part in excluded or part.startswith('.') for part in relative.parts):
            continue
        if path.suffix not in suffixes and path.name not in {'LICENSE', 'NOTICE'}:
            continue
        entries[f'relayhub-{language}/{relative.as_posix()}'] = path.read_bytes()
    if language == 'go':
        entries['relayhub-go/testdata/hmac-signing-fixtures.json'] = (STATIC / 'schemas/hmac-signing-fixtures.json').read_bytes()
    return deterministic_zip(entries)


def public_release(path):
    if not path:
        return {'product_version': KIT_VERSION, 'status': 'preview'}
    value = json.loads(Path(path).read_text())
    # Only these public fields are accepted from an internal release input.
    return {'product_version': value['product_version'], 'status': value['status']}


def build_kit(files, release, kit_output, check):
    kit_root = 'relayhub-developer-kit-v1/'
    entries = {}
    for name, data in files.items():
        entries[kit_root + name] = data
    entries[kit_root + 'manifest.json'] = json.dumps({
        'schema_version': 1,
        'product_version': release['product_version'],
        'status': release['status'],
        'contract_version': KIT_VERSION,
        'contents': sorted(name for name in files if name != 'manifest.json'),
    }, indent=2, sort_keys=True).encode() + b'\n'
    data = deterministic_zip(entries)
    write_or_check(kit_output / 'relayhub-developer-kit-v1.zip', data, check)
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--release-manifest', type=Path)
    parser.add_argument('--kit-output', type=Path)
    args = parser.parse_args()

    canonical = (STATIC / 'openapi.json').read_bytes()
    reference = SKILL / 'references/openapi.json'
    if args.check:
        assert reference.is_file() and reference.read_bytes() == canonical, 'Skill OpenAPI reference drift: run scripts/build-skill.sh'
    elif not args.output:
        write_or_check(reference, canonical, False)
    skill_files = {
        'relayhub-integration/SKILL.md': (SKILL / 'SKILL.md').read_bytes(),
        'relayhub-integration/references/authentication.md': (SKILL / 'references/authentication.md').read_bytes(),
        'relayhub-integration/references/openapi.json': canonical,
    }
    skill_zip = deterministic_zip(skill_files)
    output = args.output or STATIC / 'skills/relayhub-integration.zip'
    write_or_check(output, skill_zip, args.check)
    if args.output:
        return

    downloads = args.kit_output or STATIC / 'downloads'
    downloads.mkdir(parents=True, exist_ok=True)
    write_or_check(downloads / 'manifest.schema.json', (ROOT / 'web/docs/downloads-manifest.schema.json').read_bytes(), args.check)
    sdk_archives = {'relayhub-typescript': collect_sdk('typescript'), 'relayhub-go': collect_sdk('go')}
    for name, data in sdk_archives.items():
        write_or_check(downloads / f'{name}.zip', data, args.check)

    public_files = {
        'README.md': (ROOT / 'examples/remake/node/README.md').read_bytes(),
        'LICENSE': (ROOT / 'examples/remake/LICENSE').read_bytes(),
        'skill/relayhub-integration/SKILL.md': (SKILL / 'SKILL.md').read_bytes(),
        'skill/relayhub-integration/references/authentication.md': (SKILL / 'references/authentication.md').read_bytes(),
        'contracts/openapi.json': canonical,
        'contracts/asyncapi.yaml': (STATIC / 'asyncapi.yaml').read_bytes(),
        'contracts/manifest.json': (STATIC / 'versions/v1/contracts.json').read_bytes(),
        'examples/node/package.json': (ROOT / 'examples/remake/node/package.json').read_bytes(),
        'examples/node/package-lock.json': (ROOT / 'examples/remake/node/package-lock.json').read_bytes(),
        'examples/node/index.mjs': (ROOT / 'examples/remake/node/index.mjs').read_bytes(),
        'examples/node/README.md': (ROOT / 'examples/remake/node/README.md').read_bytes(),
        'examples/go/go.mod': (ROOT / 'examples/remake/go/go.mod').read_bytes(),
        'examples/go/main.go': (ROOT / 'examples/remake/go/main.go').read_bytes(),
        'examples/go/README.md': (ROOT / 'examples/remake/go/README.md').read_bytes(),
        'sdks/typescript/README.md': (ROOT / 'sdks/typescript/README.md').read_bytes(),
        'sdks/typescript/package.json': (ROOT / 'sdks/typescript/package.json').read_bytes(),
        'sdks/typescript/package-lock.json': (ROOT / 'sdks/typescript/package-lock.json').read_bytes(),
        'sdks/go/README.md': (ROOT / 'sdks/go/README.md').read_bytes(),
        'sdks/go/go.mod': (ROOT / 'sdks/go/go.mod').read_bytes(),
        'checks/build-node.sh': b'#!/bin/sh\nset -eu\nnpm ci --no-audit --no-fund\nnpm run build\nnpm pack --ignore-scripts\n',
        'checks/test-go.sh': b'#!/bin/sh\nset -eu\nGOWORK=off go test ./...\n',
    }
    for name, data in sdk_archives.items():
        public_files[f'sdks/{name.removeprefix("relayhub-")}.zip'] = data
        language = name.removeprefix('relayhub-')
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            prefix = f'relayhub-{language}/'
            for member in archive.namelist():
                if member.startswith(prefix) and not member.endswith('/'):
                    public_files[f'sdks/{language}/{member.removeprefix(prefix)}'] = archive.read(member)
    public_files['contracts/schemas/event-envelope.schema.json'] = (STATIC / 'schemas/event-envelope.schema.json').read_bytes()
    public_files['contracts/schemas/stream-client-frame.schema.json'] = (STATIC / 'schemas/stream-client-frame.schema.json').read_bytes()
    public_files['contracts/schemas/stream-server-frame.schema.json'] = (STATIC / 'schemas/stream-server-frame.schema.json').read_bytes()
    kit_data = build_kit(public_files, public_release(args.release_manifest), downloads, args.check)

    components = [
        ('relayhub-developer-kit', 'kit', downloads / 'relayhub-developer-kit-v1.zip', 'Node.js >=20; Go >=1.27', 'unzip and follow README.md', 'run checks/build-node.sh and checks/test-go.sh'),
        ('relayhub-integration-skill', 'skill', STATIC / 'skills/relayhub-integration.zip', 'Any agent runtime', 'extract the skill directory', 'read SKILL.md and verify one public flow'),
        ('relayhub-typescript-sdk', 'sdk', downloads / 'relayhub-typescript.zip', 'Node.js >=20', 'npm ci && npm run build', 'npm test'),
        ('relayhub-go-sdk', 'sdk', downloads / 'relayhub-go.zip', 'Go >=1.27', 'GOWORK=off go test ./...', 'GOWORK=off go test ./...'),
    ]
    def public_url(kind, path):
        return '/skills/relayhub-integration.zip' if kind == 'skill' else '/downloads/' + path.name
    manifest = {
        'schema_version': 1,
        'product_version': KIT_VERSION,
        'status': public_release(args.release_manifest)['status'],
        'contracts': {'http': '/openapi.json', 'messages': '/asyncapi.yaml'},
        'components': [
            {'name': name, 'kind': kind, 'version': KIT_VERSION, 'runtime': runtime,
             'relative_url': public_url(kind, path),
             'sha256': digest(path.read_bytes()), 'build_command': build, 'test_command': test}
            for name, kind, path, runtime, build, test in components
        ],
    }
    manifest_data = json.dumps(manifest, indent=2, sort_keys=True).encode() + b'\n'
    write_or_check(downloads / 'manifest.json', manifest_data, args.check)
    # The outer manifest is the checksum authority; never include its own hash in itself.
    if not args.check:
        (downloads / 'README.md').write_text('# RelayHub Developer Kit\n\nVerify `manifest.json` before extracting the versioned kit.\n')


if __name__ == '__main__':
    main()
PY

#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
cd "$repo_dir"
exec "${PYTHON:-python3}" - "$@" <<'PY'
import argparse, io, zipfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');p.add_argument('--output',type=Path);a=p.parse_args()
root=Path('web/docs/static'); skill=root/'skills/relayhub-integration'
canonical=(root/'openapi.json').read_bytes(); reference=skill/'references/openapi.json'
if a.check:
 assert reference.is_file() and reference.read_bytes()==canonical, 'Skill OpenAPI reference drift: run scripts/build-skill.sh'
elif not a.output:
 reference.parent.mkdir(parents=True,exist_ok=True);reference.write_bytes(canonical)
files={'relayhub-integration/SKILL.md':(skill/'SKILL.md').read_bytes(),'relayhub-integration/references/authentication.md':(skill/'references/authentication.md').read_bytes(),'relayhub-integration/references/openapi.json':canonical}
stream=io.BytesIO()
# ZIP_STORED avoids compressor/version variation. No host metadata is copied.
with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
 for name,data in sorted(files.items()):
  info=zipfile.ZipInfo(name,(1980,1,1,0,0,0));info.create_system=3;info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_STORED
  archive.writestr(info,data)
output=a.output or root/'skills/relayhub-integration.zip';expected=stream.getvalue()
if a.check: assert output.is_file() and output.read_bytes()==expected, 'Skill zip drift: run scripts/build-skill.sh'
else: output.parent.mkdir(parents=True,exist_ok=True);output.write_bytes(expected)

# SDK downloads contain only portable source/build inputs, never workspace data.
if not a.output:
 for language in ('typescript', 'go'):
  sdk=Path('sdks')/language
  excluded={'node_modules','dist','.git','coverage','.cache'}
  suffixes={'.ts','.go','.json','.mjs','.md','.mod','.sum'}
  payload=io.BytesIO()
  with zipfile.ZipFile(payload,'w',compression=zipfile.ZIP_STORED) as archive:
   for source in sorted(sdk.rglob('*')):
    relative=source.relative_to(sdk)
    if not source.is_file() or source.is_symlink() or any(part in excluded or part.startswith('.') for part in relative.parts): continue
    if source.suffix not in suffixes and source.name != 'LICENSE': continue
    entry=zipfile.ZipInfo('relayhub-'+language+'/'+relative.as_posix(),(1980,1,1,0,0,0))
    entry.create_system=3;entry.external_attr=0o100644<<16
    archive.writestr(entry,source.read_bytes())
   if language == 'go':
    fixture=zipfile.ZipInfo('relayhub-go/testdata/hmac-signing-fixtures.json',(1980,1,1,0,0,0))
    fixture.create_system=3;fixture.external_attr=0o100644<<16
    archive.writestr(fixture,(root/'schemas/hmac-signing-fixtures.json').read_bytes())
  target=root/'downloads'/('relayhub-'+language+'.zip')
  assert sdk.is_dir(), 'missing SDK source: '+str(sdk)
  if a.check: assert target.is_file() and target.read_bytes()==payload.getvalue(), 'SDK source archive drift: '+language
  else: target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(payload.getvalue())
PY

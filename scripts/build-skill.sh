#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" - "$@" <<'PY'
import argparse, io, zipfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');p.add_argument('--output',type=Path);a=p.parse_args()
root=Path('public-docs'); skill=root/'skills/relayhub-integration'
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
PY

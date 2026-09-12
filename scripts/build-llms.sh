#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" - "$@" <<'PY'
import argparse, re
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');p.add_argument('--output',type=Path);a=p.parse_args()
root=Path('public-docs');base='https://relayhub.dungxbuif.com/docs/'
# Explicit ordered content manifest. New public Markdown must be added here.
manifest=['README.md','user.md','user/getting-started.md','user/faq.md','developer.md','developer/README.md','developer/registration-flow.md','developer/auth.md','developer/api-overview.md','developer/reliability.md','developer/websocket.md','developer/streaming-protocol.md','developer/functions.md','developer/routing-realtime.md','developer/typescript-sdk.md','deploy/README.md','deploy/postgresql.md','deploy/nats.md','api.md','security.md','troubleshooting.md','skills.md','developer/skills.md','skills/relayhub-integration/SKILL.md','skills/relayhub-integration/references/authentication.md']
assert len(manifest)==len(set(manifest)), 'duplicate llms source'
assert set(manifest)=={p.relative_to(root).as_posix() for p in root.rglob('*.md')}, 'llms Markdown manifest differs from public sources'
resources=['openapi.json','asyncapi.yaml','schemas/event-envelope.schema.json','schemas/client-frame.schema.json','schemas/server-frame.schema.json','schemas/stream-client-frame.schema.json','schemas/stream-server-frame.schema.json','skills/relayhub-integration/references/openapi.json','skills/relayhub-integration.zip','llms.txt']
text='# RelayHub full integration reference\n\nCanonical Markdown, concatenated in a stable order. Each section identifies its source URL; resolve relative Markdown links against that source.\n\n'
for name in manifest:
 text+='---\n\nSource: '+base+name+'\n\n'+(root/name).read_text().rstrip()+'\n\n'
text+='---\n\n## Contract and resource index\n\n'+''.join('- '+base+name+'\n' for name in resources)
assert text.count('\nSource: ')==len(manifest), 'duplicate source headings'
output=a.output or root/'llms-full.txt'
if a.check: assert output.is_file() and output.read_text()==text, 'llms-full.txt drift: run scripts/build-llms.sh'
else: output.write_text(text)
PY

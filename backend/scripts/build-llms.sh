#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
cd "$repo_dir"
exec "${PYTHON:-python3}" - "$@" <<'PY'
import argparse, os, re
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');p.add_argument('--output',type=Path);a=p.parse_args()
root=Path('web/docs/static');base='https://relayhub.dungxbuif.com/docs/'
# Explicit ordered content manifest. New public Markdown must be added here.
manifest=['README.md','user.md','user/getting-started.md','user/faq.md','developer.md','developer/README.md','developer/registration-flow.md','developer/auth.md','developer/api-overview.md','developer/reliability.md','developer/websocket.md','developer/streaming-protocol.md','developer/functions.md','developer/routing-realtime.md','developer/typescript-sdk.md','developer/queue.md','deploy/README.md','deploy/postgresql.md','deploy/nats.md','api.md','security.md','troubleshooting.md','skills.md','developer/skills.md','skills/relayhub-integration/SKILL.md','skills/relayhub-integration/references/authentication.md']
assert len(manifest)==len(set(manifest)), 'duplicate llms source'
actual=set()
for current, dirs, files in os.walk(root):
 dirs.sort(); files.sort()
 for filename in files:
  if filename.endswith('.md'):
   actual.add((Path(current)/filename).relative_to(root).as_posix())
assert set(manifest)==actual, 'llms Markdown manifest differs from public sources'
resources=['openapi.json','asyncapi.yaml','schemas/event-envelope.schema.json','schemas/client-frame.schema.json','schemas/server-frame.schema.json','schemas/client-frame-v2.schema.json','schemas/server-frame-v2.schema.json','schemas/stream-client-frame.schema.json','schemas/stream-server-frame.schema.json','schemas/queue-subscription.schema.json','schemas/queue-delivery.schema.json','skills/relayhub-integration/references/openapi.json','skills/relayhub-integration.zip','llms.txt']
text='# RelayHub full integration reference\n\nCanonical Markdown, concatenated in a stable order. Each section identifies its source URL; resolve relative Markdown links against that source.\n\n'
text+='## Public introduction and tutorials\n\n'+(root/'public-guide.txt').read_text()+'\n\n## Detailed integration reference\n\n'
for name in manifest:
 text+='---\n\nSource: '+base+name+'\n\n'+(root/name).read_text().rstrip()+'\n\n'
text+='---\n\n## Contract and resource index\n\n'+''.join('- '+base+name+'\n' for name in resources)
assert text.count('\nSource: ')==len(manifest)+(root/'public-guide.txt').read_text().count('\nSource: '), 'duplicate source headings'
output=a.output or root/'llms-full.txt'
if a.check: assert output.is_file() and output.read_text()==text, 'llms-full.txt drift: run scripts/build-llms.sh'
else: output.write_text(text)
PY

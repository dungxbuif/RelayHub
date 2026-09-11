#!/usr/bin/env python3
import os
import re
import shutil
import tempfile
import threading
import time
import urllib.request
from http.server import SimpleHTTPRequestHandler
from pathlib import Path
from urllib.error import HTTPError, URLError
from socketserver import TCPServer

DOCS_ROOT = Path(__file__).resolve().parent.parent
LOCAL_DOCS_ROOT = DOCS_ROOT.parent / 'docs'
PORT = int(os.getenv('DOCS_SMOKE_PORT', '19080'))

REQUIRED_PAGES = [
    '/docs',
    '/docs/',
    '/docs/user',
    '/docs/user/getting-started.md',
    '/docs/user/faq.md',
    '/docs/developer',
    '/docs/developer/skills.md',
    '/docs/developer/auth.md',
    '/docs/llms.txt',
    '/docs/llms-full.txt',
]

EXPECTED_CONTENT = {
    '/docs': 'RelayHub Docs',
    '/docs/user': 'User Guide',
    '/docs/developer/skills.md': 'Skills Pack',
    '/docs/developer/auth.md': 'Auth',
    '/docs/llms.txt': 'RelayHub Docs',
}

class QuietHandler(SimpleHTTPRequestHandler):
    """Minimal request handler for smoke tests (silent logs)."""
    def log_message(self, format, *args):
        pass

def serve_docs_in_prefix(docs_dir: Path, port: int):
    root_dir = Path(tempfile.mkdtemp(prefix='relayhub-docs-smoke-'))
    target_root = root_dir / 'docs'
    shutil.copytree(docs_dir, target_root, dirs_exist_ok=True)
    (target_root / 'user').mkdir(parents=True, exist_ok=True)
    (target_root / 'developer').mkdir(parents=True, exist_ok=True)

    server = TCPServer(('127.0.0.1', port), QuietHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, root_dir

def check_http(url: str, expect_status_in=(200,), allow_redirect=False):
    if allow_redirect:
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        opener = urllib.request.build_opener(NoRedirect)
    else:
        opener = urllib.request.build_opener()

    try:
        response = opener.open(url, timeout=8)
    except HTTPError as e:
        if allow_redirect and e.code in expect_status_in:
            return e.code, e.read().decode(errors='replace')
        return e.code, e.read().decode(errors='replace')
    except URLError as e:
        raise RuntimeError(f"Cannot reach {url}: {e}")

    code = getattr(response, 'status', None) or getattr(response, 'code', None)
    body = response.read(12000).decode(errors='replace')
    response.close()
    return code, body

def assert_endpoints(base_url: str):
    errors = []

    for path in REQUIRED_PAGES:
        try:
            code, body = check_http(f'{base_url}{path}', expect_status_in=(200, 301, 302), allow_redirect=True)
        except Exception as e:
            errors.append(f'{path}: request failed ({e})')
            continue

        if code not in (200, 301, 302):
            errors.append(f'{path}: status {code}, expected 200/301/302')
            continue

        if code in (301, 302):
            # docs root should still be reachable (proxy/caddy usually redirects to /docs/)
            continue

        expected = EXPECTED_CONTENT.get(path)
        if expected and expected not in body:
            errors.append(f'{path}: missing expected content marker "{expected}"')

    return errors


def check_local_links(docs_root: Path):
    pattern = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
    errors = []
    markdown_files = sorted(docs_root.rglob('*.md'))

    for src in markdown_files:
        content = src.read_text(encoding='utf-8', errors='replace')
        for m in pattern.findall(content):
            href = m.strip()
            if not href:
                continue
            if href.startswith(('http://', 'https://', 'mailto:', '#')):
                continue
            href = href.split()[0]
            href = href.split('#')[0]
            if not href:
                continue

            if href.startswith('/'):  # absolute docs path
                target = docs_root / href.lstrip('/')
            else:
                target = (src.parent / href).resolve()

            if href.endswith('/'):
                target = target
            if not target.exists():
                # try fallback for .md links without explicit extension
                alt = target
                if not alt.suffix and (alt / 'index.md').exists():
                    continue
                errors.append(f'{src.relative_to(docs_root)} -> {href} (resolved {target})')

    return errors


def check_local_link_roots(roots: list[Path]):
    all_errors = []
    for root in roots:
        if not root.exists():
            all_errors.append(f'{root}: missing directory for link scan')
            continue
        all_errors.extend(check_local_links(root))
    return all_errors


def main():
    if not DOCS_ROOT.exists():
        raise SystemExit(f'public-docs not found at {DOCS_ROOT}')

    server, root_dir = serve_docs_in_prefix(DOCS_ROOT, PORT)
    os.chdir(root_dir)
    base = f'http://127.0.0.1:{PORT}'

    try:
        time.sleep(0.4)
        endpoint_errors = assert_endpoints(base)
        link_errors = check_local_link_roots([DOCS_ROOT, LOCAL_DOCS_ROOT])

        for e in endpoint_errors:
            print(f'[ERROR] endpoint: {e}')
        for e in link_errors:
            print(f'[ERROR] link: {e}')

        if endpoint_errors or link_errors:
            raise SystemExit(2)

        print('[OK] Docs smoke test passed')
        print(f'   endpoints checked: {len(REQUIRED_PAGES)}')
        total_md = len(list(DOCS_ROOT.rglob("*.md"))) + len(list(LOCAL_DOCS_ROOT.rglob("*.md")))
        print(f'   markdown files scanned: {total_md}')
        print(f'   link scan errors: 0')

    finally:
        server.shutdown()
        shutil.rmtree(root_dir, ignore_errors=True)

if __name__ == '__main__':
    main()

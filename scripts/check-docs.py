#!/usr/bin/env python3
"""Check contracts, reproducible artifacts, links and the actual API/Redis boundary."""
import argparse, copy, hashlib, hmac, io, json, os, re, secrets, shutil, socket, ssl, subprocess, sys, tempfile, time, urllib.parse, urllib.request, zipfile
from contextlib import contextmanager
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from openapi_spec_validator import validate as validate_openapi
ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / 'public-docs'
BASE = 'https://relayhub.dungxbuif.com/docs/'
REQUIRED = ['openapi.json', 'schemas/event-envelope.schema.json', 'schemas/client-frame.schema.json', 'schemas/server-frame.schema.json', 'skills/relayhub-integration/SKILL.md', 'skills/relayhub-integration/references/authentication.md', 'skills/relayhub-integration/references/openapi.json', 'skills/relayhub-integration.zip', 'llms.txt', 'llms-full.txt', 'assets/docs.css', 'assets/docs.js']

def run(*args, **kw):
    return subprocess.run(args, cwd=ROOT, check=True, **kw)

class HTML(HTMLParser):
    def __init__(self, text):
        super().__init__(); self.links=[]; self.ids=set(); self.tags=[]; self.feed(text)
    def handle_starttag(self, tag, attrs):
        attrs=dict(attrs); self.tags.append((tag,attrs))
        if 'id' in attrs:
            assert attrs['id'] not in self.ids, 'duplicate HTML id'
            self.ids.add(attrs['id'])
        for attr in ('href','src'):
            if attr in attrs: self.links.append(attrs[attr])

def anchors(path):
    text=path.read_text()
    if path.suffix=='.html': return HTML(text).ids
    counts={}; result=set()
    text=re.sub(r'```.*?```','',text,flags=re.S)
    for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$',text,re.M):
        slug=re.sub(r'[^\w\- ]','',heading.lower()).replace(' ','-')
        n=counts.get(slug,0); counts[slug]=n+1
        result.add(slug if n==0 else f'{slug}-{n}')
    return result

def check_links():
    for src in sorted(DOCS.rglob('*')):
        if src.suffix not in ('.md','.html') and src.name!='llms.txt': continue
        text=src.read_text()
        if src.suffix=='.html': links=HTML(text).links
        else:
            prose=re.sub(r'```.*?```|`[^`]*`','',text,flags=re.S)
            assert not re.search(r'^\s{0,3}\[[^\]]+\]:|\[[^\]]+\]\s*\[[^\]]*\]',prose,re.M), f'unsupported reference-style Markdown link: {src.relative_to(DOCS)}; use inline links'
            links=re.findall(r'\[[^\]]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)',prose)+HTML(prose).links
        for href in links:
            parsed=urllib.parse.urlsplit(href)
            if parsed.scheme and not href.startswith(BASE): continue
            if href.startswith(BASE): target=DOCS/parsed.path.removeprefix('/docs/')
            elif parsed.path.startswith('/docs/'): target=DOCS/parsed.path.removeprefix('/docs/')
            elif parsed.path.startswith('/'): continue
            else: target=src.parent/urllib.parse.unquote(parsed.path) if parsed.path else src
            target=target.resolve()
            assert target.is_relative_to(DOCS.resolve()), f'link escapes public docs: {src}: {href}'
            if target.is_dir(): target=target/'index.html'
            assert target.is_file(), f'broken link: {src.relative_to(DOCS)} -> {href}'
            if parsed.fragment: assert urllib.parse.unquote(parsed.fragment) in anchors(target), f'broken anchor: {src} -> {href}'
    html=HTML((DOCS/'index.html').read_text())
    assert any(t=='meta' and a.get('name')=='viewport' for t,a in html.tags), 'missing responsive viewport'
    assert any(t=='nav' and a.get('aria-label') for t,a in html.tags), 'missing named navigation'
    assert any(t=='a' and a.get('href')=='#main' for t,a in html.tags), 'missing keyboard skip link'
    for target in ('#user','#developer','#api','#skills'):
        assert target in html.links, f'missing console section {target}'
    buttons=[a for t,a in html.tags if t=='button' and 'data-copy' in a]
    assert buttons and all(a['data-copy'] in html.ids for a in buttons), 'invalid copy controls'
    assert any(a.get('role')=='status' for _,a in html.tags), 'missing copy status'
    assert any(t=='a' and 'download' in a and a.get('href','').endswith('.zip') for t,a in html.tags), 'missing direct download'

def check_json():
    documents={p.relative_to(DOCS).as_posix():json.loads(p.read_text()) for p in DOCS.rglob('*.json')}
    spec=documents['openapi.json']
    assert spec['openapi'].startswith('3.1.'), 'OpenAPI must be 3.1'
    validate_openapi(spec, base_uri=BASE+'openapi.json')
    ids=[]
    for path, item in spec['paths'].items():
        for method, operation in item.items():
            if method not in ('get','post','patch','delete','put','head','options'): continue
            ids.append(operation['operationId'])
            assert operation['responses'], f'no responses: {path}'
            assert 'security' in operation, f'implicit security: {path}'
    assert len(ids)==len(set(ids)), 'duplicate operation IDs'
    registry=Registry().with_resources((BASE+name, Resource.from_contents(doc,default_specification=__import__('referencing.jsonschema',fromlist=['DRAFT202012']).DRAFT202012)) for name,doc in documents.items() if name.startswith('schemas/'))
    event={'id':'evt_example','type':'order.created','source_app_id':'app_source','target_app_ids':['app_target'],'data':{},'created_at':'2026-09-11T10:00:00Z'}
    job={'id':'job_example','event_id':'evt_example','source_app_id':'app_source','target_app_id':'app_target','status':'pending','attempts':0,'created_at':event['created_at'],'updated_at':event['created_at']}
    fixtures={
      'event-envelope':([event],[{},dict(event,data=[]),dict(event,target_app_ids=[]),dict(event,created_at='yesterday'),dict(event,tenant_id='unshipped')]),
      'client-frame':([{'type':'ping'},{'type':'subscribe','topics':['events','functions']},{'type':'rpc.result','invocation_id':'inv_example','ok':True,'result':None},{'type':'rpc.result','invocation_id':'inv_example','ok':False,'error':{'code':'failed','message':'Failure'}}],[{}, {'type':'subscribe','topics':[]},{'type':'subscribe','topics':['events','events']},{'type':'ping','app_id':'app_spoof'},{'type':'rpc.result','invocation_id':'inv_example','ok':True},{'type':'rpc.result','invocation_id':'inv_example','ok':False,'error':{'code':'bad code','message':'Failure'}}]),
      'server-frame':([{'type':'ready','app_id':'app_target','connection_id':'conn_example'},{'type':'subscribed','topics':['events']},{'type':'event','event':event},{'type':'job.updated','job':job},{'type':'pong'},{'type':'error','code':'invalid_frame','message':'Invalid frame.'},{'type':'rpc.invoke','invocation_id':'inv_example','function':'calculate','input':{},'deadline':event['created_at']}],[{}, {'type':'ready'},{'type':'event','event':dict(event,data=None)},{'type':'rpc.invoke','invocation_id':'inv_example','function':'bad name','input':[],'deadline':'bad'}])}
    for name,(valid,invalid) in fixtures.items():
        schema=documents[f'schemas/{name}.schema.json']; Draft202012Validator.check_schema(schema)
        v=Draft202012Validator(schema,registry=registry,format_checker=FormatChecker())
        for fixture in valid: v.validate(fixture)
        for fixture in invalid: assert not v.is_valid(fixture), f'{name} accepted invalid fixture: {fixture}'
    check_app_contracts(spec)
    return spec

def check_app_contracts(spec):
    validators={name:Draft202012Validator(dict(spec['components']['schemas'][name],components=spec['components']),format_checker=FormatChecker()) for name in ('CreateApp','UpdateApp')}
    invalid=[{'name':'orders','delivery_mode':'callback'}, {'name':'orders','delivery_mode':'all','callback_url':None}, {'name':'x'*129,'delivery_mode':'queue'}, {'name':'   ','delivery_mode':'queue'}]
    invalid += [dict(name='orders',delivery_mode='callback',callback_url=url) for url in ['ftp://example.com/hook','https://user@example.com/hook','https://example.com/hook#fragment','https:///hook']]
    for fixture in invalid: assert not validators['CreateApp'].is_valid(fixture), f'CreateApp accepted invalid fixture: {fixture}'
    for fixture in [{'name':'x'*128,'delivery_mode':'queue'}, {'name':'  '+'x'*128+'  ','delivery_mode':'queue'}, {'name':'  orders  ','delivery_mode':'queue'}, {'name':'orders','delivery_mode':'callback','callback_url':'https://example.com/hook'}]: validators['CreateApp'].validate(fixture)
    for url in ['HTTPS://example.com/hook','https://example.com:/hook']:
        validators['CreateApp'].validate({'name':'orders','delivery_mode':'callback','callback_url':url})
        validators['UpdateApp'].validate({'callback_url':url})
    for fixture in [{'delivery_mode':'callback','callback_url':None},{'delivery_mode':'all','callback_url':None},{'name':'x'*129}]: assert not validators['UpdateApp'].is_valid(fixture), f'UpdateApp accepted invalid fixture: {fixture}'
    for fixture in [{'delivery_mode':'callback'}, {'callback_url':None}, {'delivery_mode':'queue','callback_url':None}]: validators['UpdateApp'].validate(fixture)

def check_generated():
    for script in ('build-skill.sh','build-llms.sh'): run('sh',str(ROOT/'scripts'/script),'--check')
    assert (DOCS/'openapi.json').read_bytes()==(DOCS/'skills/relayhub-integration/references/openapi.json').read_bytes(), 'Skill OpenAPI drift'
    run('go','test','./web','-count=1')
    # Build twice in isolated outputs; never repair drift while checking.
    with tempfile.TemporaryDirectory() as temp:
        a=Path(temp)/'a'; b=Path(temp)/'b'
        run('sh','scripts/build-skill.sh','--output',str(a))
        run('sh','scripts/build-skill.sh','--output',str(b))
        assert a.read_bytes()==b.read_bytes()==(DOCS/'skills/relayhub-integration.zip').read_bytes(), 'Skill zip is not reproducible'
        with zipfile.ZipFile(a) as z:
            assert z.namelist()==sorted(z.namelist()), 'zip order'
            assert z.namelist()==['relayhub-integration/SKILL.md','relayhub-integration/references/authentication.md','relayhub-integration/references/openapi.json'], 'unexpected Skill files'
            for info in z.infolist():
                assert info.date_time==(1980,1,1,0,0,0) and info.external_attr >>16==0o100644, 'zip metadata'
                assert z.read(info.filename)==(DOCS/'skills'/info.filename).read_bytes(), 'zip content drift'

def request(base,path,method='GET',body=None,headers=None):
    req=urllib.request.Request(base+path,data=body,headers=headers or {},method=method)
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args): return None
    try: r=urllib.request.build_opener(NoRedirect).open(req,timeout=40)
    except HTTPError as e: r=e
    with r: return r.status,dict(r.headers),r.read()

def free_port():
    with socket.socket() as s: s.bind(('127.0.0.1',0)); return s.getsockname()[1]

def parse_redis_test_url(raw_url):
    """Validate the supported URL subset before the app can create test state."""
    try:
        assert raw_url.split(':',1)[0] in ('redis','rediss')
        url=urllib.parse.urlsplit(raw_url)
        assert url.scheme in ('redis','rediss') and url.hostname and not url.fragment
        assert not re.search(r'[\x00-\x20\x7f]|%(?![0-9a-fA-F]{2})',raw_url)
        assert url.port is None or 1<=url.port<=65535
        assert re.fullmatch(r'(?:/[0-9]*)?',url.path)
        database=int(url.path[1:] or '0')
        assert database<=9223372036854775807
        if url.query:
            options=urllib.parse.parse_qsl(url.query,keep_blank_values=True,strict_parsing=True)
            assert len(options)==1 and options[0][0]=='db'
            assert re.fullmatch(r'[0-9]+',options[0][1])
            database=int(options[0][1])
            assert database<=9223372036854775807
        # go-redis v9.22.0 gives a nonempty db query value precedence over /db.
        # Reject other options and ambiguous forms instead of partly honoring them.
        return url,database
    except Exception:
        raise AssertionError('invalid docs-test Redis URL: use redis(s)://host[:port][/database] with at most one nonnegative decimal db query override and no other options') from None

def redis_command(raw_url, *parts):
    """Small RESP2 boundary for dependency checks and prefix-only test cleanup."""
    url,database=parse_redis_test_url(raw_url)
    try:
        connection=socket.create_connection((url.hostname,url.port or 6379),timeout=3)
        if url.scheme=='rediss': connection=ssl.create_default_context().wrap_socket(connection,server_hostname=url.hostname)
        with connection, connection.makefile('rwb') as wire:
            def command(values):
                encoded=[v if isinstance(v,bytes) else str(v).encode() for v in values]
                wire.write(b'*'+str(len(encoded)).encode()+b'\r\n'+b''.join(b'$'+str(len(v)).encode()+b'\r\n'+v+b'\r\n' for v in encoded));wire.flush()
                def read():
                    line=wire.readline()
                    if not line.endswith(b'\r\n'): raise AssertionError('incomplete Redis test response')
                    kind,data=line[:1],line[1:-2]
                    if kind==b'+': return data
                    if kind==b':': return int(data)
                    if kind==b'*': return [read() for _ in range(int(data))]
                    if kind==b'$':
                        length=int(data)
                        if length<0:return None
                        result=wire.read(length)
                        assert len(result)==length and wire.read(2)==b'\r\n', 'incomplete Redis test data'
                        return result
                    raise AssertionError('Redis test command failed')
                return read()
            if url.password is not None:
                password=urllib.parse.unquote(url.password)
                command(['AUTH',urllib.parse.unquote(url.username),password] if url.username else ['AUTH',password])
            command(['SELECT',database])
            return command(parts)
    except Exception:
        # URLs and AUTH failures must not print credentials or Redis response text.
        raise AssertionError('docs-test Redis command failed; check its URL, authentication and availability') from None

def cleanup_redis_prefix(raw_url,prefix):
    assert re.fullmatch(r'relayhubdocs_[0-9a-f]{32}',prefix), 'unsafe docs cleanup prefix'
    cursor=b'0';deadline=time.monotonic()+10
    while True:
        assert time.monotonic()<deadline, 'docs Redis cleanup deadline exceeded'
        cursor,keys=redis_command(raw_url,'SCAN',cursor,'MATCH',prefix+':*','COUNT',1000)
        assert all(key.startswith((prefix+':').encode()) for key in keys), 'Redis cleanup scope mismatch'
        if keys: redis_command(raw_url,'DEL',*keys)
        if cursor==b'0':break

@contextmanager
def runtime():
    external_url=os.getenv('RELAYHUB_DOCS_TEST_REDIS_URL','').strip()
    assert external_url or shutil.which('redis-server'), 'RELAYHUB_DOCS_TEST_REDIS_URL or host redis-server is required for real API smoke'
    if external_url:parse_redis_test_url(external_url)
    prefix='relayhubdocs_'+secrets.token_hex(16)
    with tempfile.TemporaryDirectory(prefix='relayhub-contracts-') as temp:
        temp=Path(temp); redis_port=free_port(); api_port=free_port()
        redis_url=external_url or f'redis://127.0.0.1:{redis_port}/0'
        env={k:v for k,v in os.environ.items() if not k.startswith('RELAYHUB_')}
        env.update(RELAYHUB_REDIS_URL=redis_url,RELAYHUB_HTTP_ADDR=f'127.0.0.1:{api_port}',RELAYHUB_ADMIN_TOKEN=secrets.token_hex(32),RELAYHUB_SIGNING_SECRET=secrets.token_hex(32),RELAYHUB_REDIS_KEY_PREFIX=prefix)
        run('go','build','-o',str(temp/'relayhub'),'./cmd/relayhub')
        api=redis=None;redis_ready=False
        try:
            if not external_url:
                redis=subprocess.Popen(['redis-server','--bind','127.0.0.1','--port',str(redis_port),'--save','','--appendonly','no','--dir',str(temp)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            for _ in range(100):
                try:
                    if redis_command(redis_url,'PING')==b'PONG':redis_ready=True;break
                except AssertionError:
                    if external_url:raise  # CI's explicit dependency must fail, never fall back or skip.
                    time.sleep(.05)
            assert redis_ready, 'docs-test Redis did not become ready'
            api=subprocess.Popen([str(temp/'relayhub'),'api'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            base=f'http://127.0.0.1:{api_port}'
            for _ in range(100):
                try:
                    if request(base,'/readyz')[0]==200: break
                except OSError: pass
                time.sleep(.05)
            else: raise AssertionError('real API did not become ready')
            yield base,env['RELAYHUB_ADMIN_TOKEN']
        finally:
            def stop(proc):
                if proc:
                    proc.terminate()
                    try: proc.wait(timeout=15)
                    except subprocess.TimeoutExpired: proc.kill();proc.wait(timeout=5)
            try:
                stop(api)
                if external_url and redis_ready:cleanup_redis_prefix(redis_url,prefix)
            finally:stop(redis)

AUTH_SECURITY={'public':[],'admin':[{'AdminBearer':[]}],'app':[{'AppApiKey':[],'AppSignature':[]}],'ws_token':[{'SocketToken':[]}]}

def check_route_auth(spec):
    with tempfile.TemporaryDirectory(prefix='relayhub-route-manifest-') as temp:
        output=Path(temp)/'routes.json'
        run('go','test','./internal/httpapi','-run','TestRouteManifest','-count=1',env=dict(os.environ,RELAYHUB_ROUTE_MANIFEST_OUTPUT=str(output)))
        routes=json.loads(output.read_text())
    result={}
    for route in routes:
        path='/docs/{resource}' if route['path']=='/docs/*' else route['path']
        operation=spec['paths'][path][route['method'].lower()]
        assert operation['security']==AUTH_SECURITY[route['auth']], f'OpenAPI auth category drift: {route}'
        result[(route['method'],path)]=route['auth']
    return result

def check_runtime(spec,manifest=None):
    manifest=manifest or check_route_auth(spec)
    with runtime() as (base,admin):
        status,headers,_=request(base,'/docs')
        assert status==308 and headers.get('Location')=='/docs/', 'docs redirect'
        for p in ['index.html']+REQUIRED+sorted(x.relative_to(DOCS).as_posix() for x in DOCS.rglob('*.md')):
            path='/docs/' if p=='index.html' else '/docs/'+p
            status,headers,raw=request(base,path)
            assert status==200, f'{path}: HTTP {status}'
            assert raw==(DOCS/p).read_bytes(), f'{path}: download bytes differ'
            expected={'.json':'application/json','.md':'text/markdown','.txt':'text/plain','.zip':'application/zip','.html':'text/html','.css':'text/css','.js':'javascript'}[Path(p).suffix]
            assert expected in headers.get('Content-Type',''), f'{path}: wrong MIME {headers}'
            if p.endswith('.json'):
                schema=spec['paths']['/docs/{resource}']['get']['responses']['200']['content']['application/json']['schema']
                Draft202012Validator(schema).validate(json.loads(raw))
            if p.endswith('.zip'): assert 'attachment' in headers.get('Content-Disposition',''), 'zip needs direct download header'
        adminheaders={'Authorization':'Bearer '+admin,'Content-Type':'application/json'}
        def call(path,method='GET',value=None,cred=None,admin=False,key=None,wrong_auth=False):
            body=b'' if value is None else json.dumps(value,separators=(',',':')).encode()
            headers=dict(adminheaders) if admin else {'Content-Type':'application/json'}
            if cred:
                timestamp=str(int(time.time())); canonical='\n'.join((timestamp,method,path,hashlib.sha256(body).hexdigest()))
                headers.update({'X-RelayHub-Api-Key':cred['api_key'],'X-RelayHub-Timestamp':timestamp,'X-RelayHub-Signature':hmac.new(cred['hmac_secret'].encode(),canonical.encode(),hashlib.sha256).hexdigest()})
            if key: headers['Idempotency-Key']=key
            template=re.sub(r'/(app_|evt_|job_|fn_)[^/]+',lambda m:'/{'+{'app_':'appID','evt_':'eventID','job_':'jobID','fn_':'functionID'}[m[1]]+'}',path.split('?')[0])
            operation=spec['paths'][template][method.lower()]
            category=manifest[(method,template)]
            assert operation['security']==AUTH_SECURITY[category], f'auth declaration differs from manifest: {method} {template}'
            supplied='admin' if admin else 'app' if cred else 'public'
            if not wrong_auth: assert supplied==category, f'incorrect auth fixture: {method} {template}: {supplied} vs {category}'
            status,hs,raw=request(base,path,method,body if method!='GET' else None,headers)
            if wrong_auth: assert status==401, f'wrong auth category accepted: {method} {template}: {supplied} vs {category}, HTTP {status}'
            assert str(status) in operation['responses'], f'undocumented status {method} {path}: {status}'
            data=json.loads(raw) if raw else None
            response=operation['responses'][str(status)]
            if '$ref' in response: response=spec['components']['responses'][response['$ref'].split('/')[-1]]
            if 'application/json' in response.get('content',{}):
                schema=response['content']['application/json']['schema']
                Draft202012Validator({'$ref':schema['$ref'],'components':spec['components']},format_checker=FormatChecker()).validate(data) if '$ref' in schema else Draft202012Validator(dict(schema,components=spec['components'])).validate(data)
            return status,hs,data
        _,_,a=call('/api/v1/apps','POST',{'name':'contract-source','delivery_mode':'queue'},admin=True)
        _,_,b=call('/api/v1/apps','POST',{'name':'contract-target','delivery_mode':'queue'},admin=True)
        assert call('/api/v1/apps',admin=True)[0]==200
        invalid_apps=[{'name':'bad','delivery_mode':'callback'}, {'name':'bad','delivery_mode':'all','callback_url':None}, {'name':'x'*129,'delivery_mode':'queue'}, {'name':'é'*65,'delivery_mode':'queue'}, {'name':'  ','delivery_mode':'queue'}]
        invalid_apps += [dict(name='bad',delivery_mode='callback',callback_url=url) for url in ['ftp://example.com/hook','https://user@example.com/hook','https://example.com/hook#fragment','https:///hook']]
        for fixture in invalid_apps: assert call('/api/v1/apps','POST',fixture,admin=True)[0]==400
        _,_,callback_app=call('/api/v1/apps','POST',{'name':'callback','delivery_mode':'callback','callback_url':'https://example.com/hook'},admin=True)
        callback_path='/api/v1/apps/'+callback_app['app_id']
        assert call(callback_path,'PATCH',{'callback_url':None},cred=callback_app)[0]==400
        assert call(callback_path,'PATCH',{'delivery_mode':'queue','callback_url':None},cred=callback_app)[0]==200
        assert call(callback_path,'PATCH',{'delivery_mode':'callback'},cred=callback_app)[0]==400
        assert call(callback_path,'PATCH',{'delivery_mode':'callback','callback_url':'https://example.com/hook'},cred=callback_app)[0]==200
        app='/api/v1/apps/'+a['app_id']
        assert call(app,cred=a)[0]==200
        assert call(app,'PATCH',{'name':'renamed'},cred=a)[0]==200
        status,_,socket_token=call('/api/v1/socket/token','POST',{'scopes':['ws:connect'],'ttl_seconds':60},cred=a)
        assert status==201
        address=urllib.parse.urlsplit(base)
        with socket.create_connection((address.hostname,address.port),timeout=5) as connection:
            wire=connection.makefile('rwb')
            target='/ws?token='+urllib.parse.quote(socket_token['token'],safe='')
            wire.write(('GET '+target+' HTTP/1.1\r\nHost: '+address.netloc+'\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n').encode()); wire.flush()
            assert wire.readline().startswith(b'HTTP/1.1 101 '), 'WebSocket handshake'
            upgrade_headers={}
            while True:
                line=wire.readline()
                if line==b'\r\n': break
                assert line, 'incomplete WebSocket handshake'
                key,value=line.decode().split(':',1);upgrade_headers[key.lower()]=value.strip()
            assert upgrade_headers.get('sec-websocket-accept')=='s3pPLMBiTxaQ9kYGzzhZRbK+xOo=', 'WebSocket challenge'
            header=wire.read(2);assert header[0]==0x81, 'expected ready text frame'
            size=header[1]&127
            if size==126: size=int.from_bytes(wire.read(2),'big')
            assert size<65536, 'oversized ready frame'
            ready=json.loads(wire.read(size));assert ready['type']=='ready' and ready['app_id']==a['app_id'] and ready['connection_id'], 'WebSocket app identity'
            wire.close()
        payload={'type':'order.created','target_app_ids':[b['app_id']],'data':{'order_id':42}}
        status,_,pub=call('/api/v1/events','POST',payload,cred=a,key='docs-event')
        assert status==202
        assert call('/api/v1/events','POST',payload,cred=a,key='docs-event')[1].get('Idempotent-Replayed')=='true'
        event='/api/v1/events/'+pub['event']['id']; job='/api/v1/jobs/'+pub['jobs'][0]['id']
        assert call(event,cred=b)[0]==200
        assert call(job,cred=b)[0]==200
        assert call('/api/v1/queue?limit=1&wait=0',cred=b)[2][0]['event']['id']==pub['event']['id']
        assert call(job+'/dead-letter','POST',admin=True)[0]==200
        assert call(job+'/requeue','POST',admin=True)[0]==200
        assert call(event+'/ack','POST',cred=b)[0]==204
        status,_,function=call('/api/v1/functions','POST',{'name':'calculate','timeout_seconds':1},cred=b)
        assert status==201
        assert call('/api/v1/functions',cred=b)[0]==200
        fn='/api/v1/functions/'+function['id']
        assert call(fn+'/invoke','POST',{'input':{}},cred=a,key='docs-function')[0]==503
        assert call(fn,'DELETE',cred=b)[0]==204
        assert call(app+'/rotate-secret','POST',admin=True)[0]==200
        assert call(app,'DELETE',admin=True)[0]==200
        replacements={'{appID}':'app_missing','{eventID}':'evt_missing','{jobID}':'job_missing','{functionID}':'fn_missing'}
        for (method,path),category in manifest.items():
            if category=='public': continue
            target=path
            for key,value in replacements.items(): target=target.replace(key,value)
            assert request(base,target,method)[0]==401, f'missing-auth boundary: {method} {path}'
            if category=='admin': call(target,method,cred=b,wrong_auth=True)
            elif category=='app': call(target,method,admin=True,wrong_auth=True)
            elif category=='ws_token':
                call(target,method,admin=True,wrong_auth=True)
                call(target,method,cred=b,wrong_auth=True)
        for path in ('/healthz','/readyz','/metrics'): assert request(base,path)[0]==200
        assert request(base,'/docs/missing.json')[0]==404

def check_console():
    """Execute shipped copy logic with controlled clipboard/selection boundaries."""
    assert shutil.which('node'), 'node is required for console behavior checks (test tooling only)'
    run('node','-',str(DOCS/'assets/docs.js'),input=r'''
const fs = require('fs'), vm = require('vm'), assert = require('assert/strict');
const source = fs.readFileSync(process.argv[2], 'utf8');
async function scenario({clipboard=true, denied=false, fallback=true, fetchOK=true, sourceButton=false}={}) {
  let listener, copied, selected, removed=false, focused=null;
  const target={textContent:'literal copy text',value:sourceButton?'':'',tagName:sourceButton?'TEXTAREA':'PRE',hidden:true,focus(){focused='target'},select(){selected=this.value}};
  const status={textContent:''};
  const button={dataset:{copy:'target',...(sourceButton?{source:'skill.md'}:{})},disabled:false,addEventListener(type,fn){assert.equal(type,'click');listener=fn},focus(){if(!this.disabled)focused='button'}};
  const document={getElementById(id){return id==='copy-status'?status:target},querySelectorAll(){return [button]},body:{appendChild(){}},createElement(){return {value:'',setAttribute(){},select(){selected=this.value},remove(){removed=true}}},execCommand(){return fallback}};
  const navigator={...(clipboard?{clipboard:{async writeText(value){if(denied)throw Error('denied');copied=value}}}:{})};
  vm.runInNewContext(source,{document,navigator,window:{isSecureContext:clipboard},fetch:async()=>({ok:fetchOK,text:async()=> 'full skill source'})});
  await listener();
  assert.equal(button.disabled,false);
  assert.equal(focused,(!clipboard||denied)&&!fallback&&sourceButton&&fetchOK?'target':'button','focus restored after enabling; manual textarea keeps focus');
  if(!fetchOK) {assert.match(status.textContent,/Could not load/);return;}
  if(clipboard&&!denied) {assert.equal(copied,sourceButton?'full skill source':'literal copy text');assert.match(status.textContent,/Copied/);assert.equal(button.textContent,'Copied');}
  else if(fallback) {assert.equal(selected,sourceButton?'full skill source':'literal copy text');assert(removed&&focused);assert.match(status.textContent,/Copied/);}
  else {assert.match(status.textContent,/Automatic copy unavailable/);if(sourceButton){assert.equal(target.hidden,false);assert.equal(selected,'full skill source');assert.equal(focused,'target');}else{assert.equal(focused,'button');}}
}
(async()=>{await scenario();await scenario({clipboard:false});await scenario({denied:true});await scenario({sourceButton:true});await scenario({sourceButton:true,clipboard:false,fallback:false});await scenario({sourceButton:true,fetchOK:false});await scenario({clipboard:false,fallback:false});console.log('PASS: console clipboard, denied/insecure fallback, manual selection and fetch failure');})().catch(e=>{console.error(e);process.exit(1)});
''',text=True)

def check_negative_controls():
    """Mutation checks run in a disposable copy; never edit the working sources."""
    global ROOT,DOCS
    original_root,original_docs=ROOT,DOCS
    with tempfile.TemporaryDirectory(prefix='relayhub-negative-') as temp:
        temp=Path(temp)
        for name in ('public-docs','scripts','web','internal','cmd','.github'):
            shutil.copytree(ROOT/name,temp/name,ignore=shutil.ignore_patterns('__pycache__'))
        for name in ('go.mod','go.sum','compose.yaml','Dockerfile','.env.example'): shutil.copyfile(ROOT/name,temp/name)
        ROOT,DOCS=temp,temp/'public-docs'
        def rejects(label,action):
            try: action()
            except Exception: print('PASS negative control:',label)
            else: raise AssertionError('checker missed '+label)
        def mutate(name,change,check,label):
            path=DOCS/name; before=path.read_bytes()
            try: path.write_bytes(change(before));rejects(label,check)
            finally: path.write_bytes(before)
        quiet=dict(stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            for name,script in [('skills/relayhub-integration.zip','build-skill.sh'),('skills/relayhub-integration/references/openapi.json','build-skill.sh'),('llms-full.txt','build-llms.sh')]:
                mutate(name,lambda b:b+b'\nDRIFT',lambda:run('sh',str(ROOT/'scripts'/script),'--check',**quiet),name+' drift')
            mutate('index.html',lambda b:b.replace(b'href="#skills"',b'href="#missing-anchor"'),check_links,'HTML anchor')
            mutate('user.md',lambda b:b+b'\n[bad](missing.md)\n',check_links,'Markdown link')
            def invalid_ref(raw):
                spec=json.loads(raw);spec['paths']['/api/v1/apps']['get']['responses']['200']['content']['application/json']['schema']={'$ref':'#/components/schemas/Missing'};return json.dumps(spec).encode()
            mutate('openapi.json',invalid_ref,check_json,'OpenAPI reference')
            def remove_route(raw):
                spec=json.loads(raw);del spec['paths']['/api/v1/queue'];return json.dumps(spec).encode()
            mutate('openapi.json',remove_route,lambda:run('go','test','./internal/httpapi','-run','TestRouteManifest','-count=1',**quiet),'router coverage')
            def swap_auth(raw):
                spec=json.loads(raw)
                admin=spec['paths']['/api/v1/apps']['get'];app=spec['paths']['/api/v1/queue']['get']
                admin['security'],app['security']=app['security'],admin['security']
                return json.dumps(spec).encode()
            mutate('openapi.json',swap_auth,lambda:run('go','test','./internal/httpapi','-run','TestRouteManifest','-count=1',**quiet),'admin/app auth swap')
            mutate('schemas/event-envelope.schema.json',lambda b:b'{}',check_json,'schema accepts invalid fixtures')
            quiet_deployment=lambda:run('go','test','./cmd/relayhub','-run','TestDeploymentContract|TestCIContract','-count=1',**quiet)
            mutate('deploy/docker-compose.relayhub.yml',lambda b:b+b'\n# drift\n',quiet_deployment,'root/public Compose drift')
            for filename,before_value,after_value,label in [
                ('compose.yaml',b'read_only: true',b'read_only: false','container hardening'),
                ('.env.example',b'RELAYHUB_REDIS_PASSWORD=\n',b'RELAYHUB_REDIS_PASSWORD=usable-secret\n','example credentials'),
                ('.github/workflows/ci.yml',b'go test -race -tags=integration',b'go test -race -tags=disabled','required Redis CI gate'),
                ('.github/workflows/ci.yml',b'RELAYHUB_DOCS_TEST_REDIS_URL:',b'UNUSED_DOCS_REDIS_URL:','required docs Redis dependency')]:
                path=ROOT/filename;before=path.read_bytes();public=DOCS/'deploy/docker-compose.relayhub.yml';original_public=public.read_bytes()
                try:
                    path.write_bytes(before.replace(before_value,after_value))
                    if filename=='compose.yaml': public.write_bytes(path.read_bytes())
                    rejects(label,quiet_deployment)
                finally: path.write_bytes(before);public.write_bytes(original_public)

            embed=ROOT/'web/embed.go';embed.write_bytes(embed.read_bytes()+b'\n// drift\n')
            rejects('web/embed.go drift',lambda:run('go','test','./web','-count=1',**quiet))
        finally: ROOT,DOCS=original_root,original_docs


def check_deployment():
    """Use the parsed Go YAML contract as the single deployment/CI validator."""
    run('go', 'test', './cmd/relayhub', '-run', 'TestDeploymentContract|TestCIContract', '-count=1')

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--static',action='store_true'); parser.add_argument('--self-test',action='store_true'); args=parser.parse_args()
    missing=[p for p in REQUIRED if not (DOCS/p).is_file()]
    assert not missing, 'missing required artifacts: '+', '.join(missing)
    spec=check_json(); check_links(); check_generated(); check_console(); check_deployment()
    if args.self_test: check_negative_controls()
    manifest=check_route_auth(spec)
    if not args.static: check_runtime(spec,manifest)
    print('PASS: parsed contracts, schema fixtures, links, reproducible resources, route parity'+('' if args.static else ', real API/Redis artifacts and signed flows'))
if __name__=='__main__': main()

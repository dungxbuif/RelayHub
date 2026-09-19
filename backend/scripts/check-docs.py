#!/usr/bin/env python3
"""Check contracts, reproducible artifacts, links and the documented API boundary."""
import argparse, base64, copy, hashlib, hmac, io, json, os, re, secrets, shutil, socket, ssl, subprocess, sys, tempfile, time, urllib.parse, urllib.request, zipfile
from contextlib import contextmanager
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from openapi_spec_validator import validate as validate_openapi
import yaml
ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
DOCS = REPO / 'web/docs/static'
ADMIN = REPO / 'web/admin/legacy'
BASE = 'https://relayhub.dungxbuif.com/docs/'
REQUIRED = ['openapi.json', 'asyncapi.yaml', 'schemas/event-envelope.schema.json', 'schemas/client-frame.schema.json', 'schemas/server-frame.schema.json', 'schemas/stream-client-frame.schema.json', 'schemas/stream-server-frame.schema.json', 'skills/relayhub-integration/SKILL.md', 'skills/relayhub-integration/references/authentication.md', 'skills/relayhub-integration/references/openapi.json', 'skills/relayhub-integration.zip', 'llms.txt', 'llms-full.txt']
ADMIN_REQUIRED = ['console.html', 'assets/console.js', 'assets/docs.css', 'assets/docs.js']

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
            # Definitions inside blockquotes/lists still create shortcut links.
            # Recognize repeated container markers before rejecting this syntax.
            reference=r'^[ \t]*(?:(?:>[ \t]*|(?:[-+*]|[0-9]{1,9}[.)])[ \t]+)[ \t]*)*\[[^\]]+\]:|\[[^\]]+\]\s*\[[^\]]*\]'
            assert not re.search(reference,prose,re.M), f'unsupported reference-style Markdown link: {src.relative_to(DOCS)}; use inline links'
            links=re.findall(r'\[[^\]]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)',prose)+HTML(prose).links
        for href in links:
            parsed=urllib.parse.urlsplit(href)
            if parsed.path in ('/docs/', '/docs/developer/skills-tab', '/admin/'):
                continue
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
    check_stream_contracts(documents, registry)
    return spec

def check_stream_contracts(documents, registry):
    client_schema=documents['schemas/stream-client-frame.schema.json']
    server_schema=documents['schemas/stream-server-frame.schema.json']
    client=Draft202012Validator(client_schema,registry=registry,format_checker=FormatChecker())
    server=Draft202012Validator(server_schema,registry=registry,format_checker=FormatChecker())
    fixture_root=ROOT/'internal/streamprotocol/fixtures'
    expected_types={
      'client.valid.json':{'consumer.start','delivery.ack','delivery.nack','delivery.progress','function.result','ping'},
      'server.valid.json':{'ready','consumer.started','event.delivery','delivery.accepted','function.invoke','error','pong'},
    }
    for name,validator in [('client.valid.json',client),('server.valid.json',server)]:
        values=json.loads((fixture_root/name).read_text())
        assert values, f'empty stream fixture {name}'
        assert {value['type'] for value in values}==expected_types[name], f'stream fixture type coverage drift: {name}'
        for value in values: validator.validate(value)
    for name,validator in [('client.invalid.json',client),('server.invalid.json',server)]:
        cases=json.loads((fixture_root/name).read_text())
        assert cases, f'empty stream fixture {name}'
        for case in cases:
            assert not validator.is_valid(case['frame']), f"stream schema accepted invalid fixture: {case['name']}"
    wire=json.loads((fixture_root/'wire.invalid.json').read_text())
    required={'duplicate top-level key','duplicate nested key','not an object','two JSON values','invalid UTF-8','oversize message','wrong application ownership','missing invocation assignment','cross-application invocation assignment','stale-session invocation assignment'}
    assert {case['name'] for case in wire}==required, 'stream wire boundary fixture drift'
    invalid_utf8=next(case for case in wire if case['name']=='invalid UTF-8')
    assert not __import__('codecs').decode(base64.b64decode(invalid_utf8['wire_base64']),'utf-8','ignore'), 'invalid UTF-8 fixture drift'
    stable=set(server_schema['$defs']['error']['properties']['code']['enum'])
    assert {case['error_code'] for case in json.loads((fixture_root/'client.invalid.json').read_text())}.issubset(stable), 'fixture error code drift'
    public=(DOCS/'developer/streaming-protocol.md').read_text()
    internal=(ROOT.parent/'docs/developer/streaming-protocol.md').read_text()
    for code in stable:
        assert f'`{code}`' in public and f'`{code}`' in internal, f'undocumented stream error {code}'
    forbidden={'subject','stream_sequence','consumer_sequence','durable_name','nats_subject'}
    def property_names(value):
        if isinstance(value,dict):
            result=set(value.get('properties',{}))
            for nested in value.values(): result |= property_names(nested)
            return result
        if isinstance(value,list):
            result=set()
            for nested in value: result |= property_names(nested)
            return result
        return set()
    assert not forbidden & (property_names(client_schema)|property_names(server_schema)), 'broker field leaked into public stream schema'
    for result in (None, True, 42, 'done', [1,{'ok':True}], {'total':42}):
        client.validate({'type':'function.result','invocation_id':'inv_example','ok':True,'result':result})
    for code in ('Retry_1','_internal','A.b-c'):
        client.validate({'type':'function.result','invocation_id':'inv_example','ok':False,'error':{'code':code,'message':'Failure'}})
    long_type='event.'*100
    assert not client.is_valid({'type':'consumer.start','protocol_version':1,'consumer':'default','topics':[long_type],'max_in_flight':1}), 'reserved topic filter accepted'
    server.validate({'type':'event.delivery','delivery_id':'dlv_example','attempt':1,'event':{'id':'evt_example','type':long_type,'source_app_id':'app_source','target_app_ids':['app_target'],'data':{},'created_at':'2026-09-12T10:00:00Z'}})
    asyncapi=yaml.safe_load((DOCS/'asyncapi.yaml').read_text())
    assert asyncapi['asyncapi']=='3.0.0' and asyncapi['info']['version']=='1.0.0', 'AsyncAPI version drift'
    assert asyncapi['servers']['production']['pathname']=='/api/v1/stream', 'AsyncAPI stream path drift'
    assert asyncapi['x-relayhub-websocket-subprotocol']=='relayhub.stream.v1', 'AsyncAPI subprotocol drift'
    assert asyncapi['x-relayhub-message-limit-bytes']==65536, 'AsyncAPI message limit drift'
    assert asyncapi['components']['messages']['clientFrame']['payload']['$ref']=='./schemas/stream-client-frame.schema.json'
    assert asyncapi['components']['messages']['serverFrame']['payload']['$ref']=='./schemas/stream-server-frame.schema.json'

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

def parse_postgres_test_url(raw_url):
    """Validate the supported PostgreSQL URL subset before the app can create test state."""
    try:
        assert raw_url.split(':',1)[0] in ('postgres','postgresql')
        url=urllib.parse.urlsplit(raw_url)
        assert url.scheme in ('postgres','postgresql') and url.hostname and not url.fragment
        assert not re.search(r'[\x00-\x20\x7f]|%(?![0-9a-fA-F]{2})',raw_url)
        assert url.port is None or 1<=url.port<=65535
        assert url.path and url.path != '/'
        return url
    except Exception:
        raise AssertionError('invalid docs-test PostgreSQL URL: use postgres://user:password@host[:port]/database?... with no fragment or control characters') from None

@contextmanager
def runtime():
    postgres_url=os.getenv('RELAYHUB_DOCS_TEST_POSTGRES_URL','').strip()
    external_nats_url=os.getenv('RELAYHUB_DOCS_TEST_NATS_URL','').strip()
    assert postgres_url, 'RELAYHUB_DOCS_TEST_POSTGRES_URL is required for real API smoke'
    parse_postgres_test_url(postgres_url)
    with tempfile.TemporaryDirectory(prefix='relayhub-contracts-') as temp:
        temp=Path(temp); nats_port=free_port(); api_port=free_port()
        nats_url=external_nats_url or f'nats://127.0.0.1:{nats_port}'
        env={k:v for k,v in os.environ.items() if not k.startswith('RELAYHUB_')}
        env.update(RELAYHUB_POSTGRES_URL=postgres_url,RELAYHUB_NATS_URL=nats_url,RELAYHUB_HTTP_ADDR=f'127.0.0.1:{api_port}',RELAYHUB_ADMIN_TOKEN=secrets.token_hex(32),RELAYHUB_SIGNING_SECRET=secrets.token_hex(32),RELAYHUB_SECRET_ENCRYPTION_KEY=secrets.token_hex(32))
        run('go','build','-o',str(temp/'relayhub'),'./cmd/relayhub')
        if not external_nats_url: run('go','build','-o',str(temp/'nats-server'),'github.com/nats-io/nats-server/v2')
        api=nats=None
        try:
            if not external_nats_url:
                nats_store=temp/'nats';nats_store.mkdir()
                nats=subprocess.Popen([str(temp/'nats-server'),'-js','-a','127.0.0.1','-p',str(nats_port),'-sd',str(nats_store)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
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
            stop(api); stop(nats)

AUTH_SECURITY={'public':[],'admin':[{'AdminBearer':[]}],'app':[{'AppApiKey':[],'AppSignature':[]}],'ws_token':[{'SocketToken':[]}]}

def check_route_auth(spec):
    with tempfile.TemporaryDirectory(prefix='relayhub-route-manifest-') as temp:
        output=Path(temp)/'routes.json'
        run('go','test','./internal/httpapi','-run','TestRouteManifest','-count=1',env=dict(os.environ,RELAYHUB_ROUTE_MANIFEST_OUTPUT=str(output)))
        routes=json.loads(output.read_text())
    result={}
    for route in routes:
        if route['path'] in ('/admin','/admin/*'):
            assert route['auth']=='public', f'Admin asset route must be public: {route}'
            continue
        path=route['path']
        operation=spec['paths'][path][route['method'].lower()]
        assert operation['security']==AUTH_SECURITY[route['auth']], f'OpenAPI auth category drift: {route}'
        result[(route['method'],path)]=route['auth']
    return result

def check_runtime(spec,manifest=None):
    manifest=manifest or check_route_auth(spec)
    with runtime() as (base,admin):
        status,headers,_=request(base,'/admin')
        assert status==308 and headers.get('Location')=='/admin/', 'Admin redirect'
        for p in ADMIN_REQUIRED:
            path='/admin/'+p
            status,headers,raw=request(base,path)
            assert status==200, f'{path}: HTTP {status}'
            assert raw==(ADMIN/p).read_bytes(), f'{path}: download bytes differ'
            expected={'.html':'text/html','.css':'text/css','.js':'javascript'}[Path(p).suffix]
            assert expected in headers.get('Content-Type',''), f'{path}: wrong MIME {headers}'
        assert request(base,'/docs/intro')[0]==404, 'backend must not serve standalone docs'
        adminheaders={'Authorization':'Bearer '+admin,'Content-Type':'application/json'}
        def call(path,method='GET',value=None,cred=None,admin=False,key=None,wrong_auth=False):
            body=b'' if value is None else json.dumps(value,separators=(',',':')).encode()
            headers=dict(adminheaders) if admin else {'Content-Type':'application/json'}
            if cred:
                timestamp=str(int(time.time())); canonical='\n'.join((timestamp,method,path,hashlib.sha256(body).hexdigest()))
                headers.update({'X-RelayHub-Api-Key':cred['api_key'],'X-RelayHub-Timestamp':timestamp,'X-RelayHub-Signature':hmac.new(cred['hmac_secret'].encode(),canonical.encode(),hashlib.sha256).hexdigest()})
            if key: headers['Idempotency-Key']=key
            template=re.sub(r'/(app_|evt_|job_|fn_|rr_)[^/]+',lambda m:'/{'+{'app_':'appID','evt_':'eventID','job_':'jobID','fn_':'functionID','rr_':'ruleID'}[m[1]]+'}',path.split('?')[0])
            template=re.sub(r'/realtime/channels/[^/]+/publish$', '/realtime/channels/{channel}/publish', template)
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
        status,_,socket_token=call('/api/v1/socket/token','POST',{'scopes':['ws:connect','ws:subscribe','ws:read'],'ttl_seconds':60},cred=a)
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
        status,_,rule=call('/api/v1/routing/rules','POST',{'event_type':'order.created','target_app_id':b['app_id'],'realtime_channel':'orders'},admin=True)
        assert status==201 and rule['id']
        assert call('/api/v1/routing/rules',admin=True)[0]==200
        payload={'type':'order.created','data':{'order_id':42}}
        status,_,pub=call('/api/v1/events','POST',payload,cred=a,key='docs-event')
        assert status==202
        assert call('/api/v1/events','POST',payload,cred=a,key='docs-event')[1].get('Idempotent-Replayed')=='true'
        event='/api/v1/events/'+pub['event']['id']; job='/api/v1/jobs/'+pub['jobs'][0]['id']
        assert call(event,cred=b)[0]==200
        assert call(job,cred=b)[0]==200
        assert call('/api/v1/realtime/channels/orders/publish','POST',{'data':{'order_id':42}},cred=a)[0]==202
        assert call('/api/v1/routing/rules/'+rule['id'],'PATCH',{'enabled':False},admin=True)[0]==200
        assert call('/api/v1/routing/rules/'+rule['id'],'DELETE',admin=True)[0]==204
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
        assert request(base,'/admin/missing')[0]==404

def check_console():
    """Execute shipped copy logic with controlled clipboard/selection boundaries."""
    assert shutil.which('node'), 'node is required for console behavior checks (test tooling only)'
    run('node','-',str(ADMIN/'assets/docs.js'),input=r'''
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
    global ROOT,REPO,DOCS,ADMIN
    original_root,original_repo,original_docs,original_admin=ROOT,REPO,DOCS,ADMIN
    with tempfile.TemporaryDirectory(prefix='relayhub-negative-') as temp:
        temp=Path(temp); temp_backend=temp/'backend'
        for name in ('scripts','web','internal','cmd','deploy'):
            shutil.copytree(ROOT/name,temp_backend/name,ignore=shutil.ignore_patterns('__pycache__'))
        for name in ('go.mod','go.sum'): shutil.copyfile(ROOT/name,temp_backend/name)
        shutil.copytree(DOCS,temp/'web/docs/static')
        shutil.copytree(ADMIN,temp/'web/admin/legacy')
        (temp/'docs/developer').mkdir(parents=True)
        shutil.copyfile(REPO/'docs/developer/streaming-protocol.md',temp/'docs/developer/streaming-protocol.md')
        for name in ('compose.yaml','Dockerfile','.dockerignore','.env.example'): shutil.copyfile(REPO/name,temp/name)
        ROOT,REPO,DOCS,ADMIN=temp_backend,temp,temp/'web/docs/static',temp/'web/admin/legacy'
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
            mutate('user.md',lambda b:b+b'\n[bad](missing.md)\n',check_links,'Markdown link')
            mutate('user.md',lambda b:b+b'\n> [Missing reference]\n>\n> [Missing reference]: missing-review-target.md\n',check_links,'blockquote shortcut reference')
            def invalid_ref(raw):
                spec=json.loads(raw);spec['paths']['/api/v1/apps']['get']['responses']['200']['content']['application/json']['schema']={'$ref':'#/components/schemas/Missing'};return json.dumps(spec).encode()
            mutate('openapi.json',invalid_ref,check_json,'OpenAPI reference')
            def remove_route(raw):
                spec=json.loads(raw);del spec['paths']['/api/v1/events'];return json.dumps(spec).encode()
            mutate('openapi.json',remove_route,lambda:run('go','test','./internal/httpapi','-run','TestRouteManifest','-count=1',**quiet),'router coverage')
            def swap_auth(raw):
                spec=json.loads(raw)
                admin=spec['paths']['/api/v1/apps']['get'];app=spec['paths']['/api/v1/events/{eventID}']['get']
                admin['security'],app['security']=app['security'],admin['security']
                return json.dumps(spec).encode()
            mutate('openapi.json',swap_auth,lambda:run('go','test','./internal/httpapi','-run','TestRouteManifest','-count=1',**quiet),'admin/app auth swap')
            mutate('schemas/event-envelope.schema.json',lambda b:b'{}',check_json,'schema accepts invalid fixtures')
            mutate('schemas/stream-client-frame.schema.json',lambda b:b'{}',check_json,'stream schema accepts invalid fixtures')
            mutate('asyncapi.yaml',lambda b:b.replace(b'/api/v1/stream',b'/api/v2/stream'),check_json,'AsyncAPI stream path drift')
            fixtures=ROOT/'internal/streamprotocol/fixtures/client.valid.json';before=fixtures.read_bytes()
            try:
                values=json.loads(before);fixtures.write_text(json.dumps(values[:-1]))
                rejects('stream fixture frame coverage',check_json)
            finally:fixtures.write_bytes(before)
            quiet_deployment=lambda:run('go','test','./cmd/relayhub','-run','TestDeploymentContract','-count=1',**quiet)
            mutate('deploy/docker-compose.relayhub.yml',lambda b:b+b'\n# drift\n',quiet_deployment,'root/public Compose drift')
            for filename,before_value,after_value,label in [
                ('compose.yaml',b'read_only: true',b'read_only: false','container hardening'),
                ('.env.example',b'RELAYHUB_POSTGRES_PASSWORD=\n',b'RELAYHUB_POSTGRES_PASSWORD=usable-secret\n','example credentials')]:
                path=REPO/filename;before=path.read_bytes();public=DOCS/'deploy/docker-compose.relayhub.yml';original_public=public.read_bytes()
                try:
                    path.write_bytes(before.replace(before_value,after_value))
                    if filename=='compose.yaml': public.write_bytes(path.read_bytes())
                    rejects(label,quiet_deployment)
                finally: path.write_bytes(before);public.write_bytes(original_public)


            embed=ROOT/'web/embed.go';embed.write_bytes(embed.read_bytes()+b'\n// drift\n')
            rejects('web/embed.go drift',lambda:run('go','test','./web','-count=1',**quiet))
        finally: ROOT,REPO,DOCS,ADMIN=original_root,original_repo,original_docs,original_admin


def check_deployment():
    """Use the parsed Go YAML contract as the deployment validator."""
    run('go', 'test', './cmd/relayhub', '-run', 'TestDeploymentContract', '-count=1')

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--static',action='store_true'); parser.add_argument('--self-test',action='store_true'); args=parser.parse_args()
    missing=[p for p in REQUIRED if not (DOCS/p).is_file()]
    missing += [f'Admin:{p}' for p in ADMIN_REQUIRED if not (ADMIN/p).is_file()]
    assert not missing, 'missing required artifacts: '+', '.join(missing)
    spec=check_json(); check_links(); check_generated(); check_console(); check_deployment()
    if args.self_test: check_negative_controls()
    manifest=check_route_auth(spec)
    if not args.static: check_runtime(spec,manifest)
    print('PASS: parsed contracts, schema fixtures, links, reproducible resources, route parity'+('' if args.static else ', real API/PostgreSQL/NATS artifacts and signed flows'))
if __name__=='__main__': main()

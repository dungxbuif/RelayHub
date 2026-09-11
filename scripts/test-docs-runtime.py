#!/usr/bin/env python3
"""Real external-Redis runtime isolation checks; no host Redis binary is used."""
import importlib.util, io, json, os, secrets, shutil, socket, tempfile, urllib.parse, unittest
from pathlib import Path
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('check_docs',Path(__file__).with_name('check-docs.py'))
checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)

def redis(*parts, database=None):
    """Independent RESP test client for CI's explicit unauthenticated service."""
    url=urllib.parse.urlsplit(os.environ['RELAYHUB_DOCS_TEST_REDIS_URL'])
    with socket.create_connection((url.hostname,url.port or 6379),timeout=3) as sock, sock.makefile('rwb') as wire:
        def command(values):
            encoded=[v if isinstance(v,bytes) else str(v).encode() for v in values]
            wire.write(b'*'+str(len(encoded)).encode()+b'\r\n'+b''.join(b'$'+str(len(v)).encode()+b'\r\n'+v+b'\r\n' for v in encoded));wire.flush()
            def read():
                line=wire.readline();kind,data=line[:1],line[1:-2]
                if kind==b'-': raise AssertionError('Redis test command rejected')
                if kind==b'+':return data
                if kind==b':':return int(data)
                if kind==b'*':return [read() for _ in range(int(data))]
                if kind==b'$':
                    n=int(data)
                    if n<0:return None
                    out=wire.read(n);assert wire.read(2)==b'\r\n';return out
                raise AssertionError('invalid Redis test response')
            return read()
        if url.password: command(['AUTH',urllib.parse.unquote(url.password)])
        if database is not None:command(['SELECT',database])
        elif url.path.strip('/'):command(['SELECT',url.path.strip('/')])
        return command(parts)

class DocumentLinks(unittest.TestCase):
    def test_reference_syntax_and_embedded_html_links_cannot_hide_broken_targets(self):
        for content in ('[Missing][target]\n\n[target]: missing.md\n', '<a href="missing.md">Missing</a>', '<img src="missing.png">'):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                shutil.copytree(checker.DOCS, root, dirs_exist_ok=True)
                with (root/'README.md').open('a') as output: output.write('\n'+content)
                with patch.object(checker,'DOCS',root), self.assertRaisesRegex(AssertionError,'unsupported reference-style|broken link.*README.md'):
                    checker.check_links()

class RedisURLSelection(unittest.TestCase):
    def test_uppercase_schemes_fail_before_runtime(self):
        for scheme in ('REDIS', 'REDISS', 'Redis'):
            with self.subTest(scheme=scheme), patch.dict(os.environ, {'RELAYHUB_DOCS_TEST_REDIS_URL':scheme+'://user:SENTINEL@redis.invalid:6379/0'}), patch.object(checker,'run',side_effect=AssertionError('process before validation')):
                with self.assertRaisesRegex(AssertionError, 'invalid docs-test Redis URL') as error:
                    with checker.runtime(): pass
                self.assertNotIn('SENTINEL', str(error.exception))

    def test_query_database_overrides_path_on_the_wire(self):
        class Connection:
            def __init__(self):self.sent=io.BytesIO();self.responses=io.BytesIO(b'+OK\r\n+PONG\r\n')
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def makefile(self,*args):return self
            def write(self,data):return self.sent.write(data)
            def flush(self):pass
            def readline(self):return self.responses.readline()
        connection=Connection()
        with patch.object(checker.socket,'create_connection',return_value=connection):
            self.assertEqual(checker.redis_command('redis://redis.invalid:6379/0?db=1','PING'),b'PONG')
        self.assertEqual(connection.sent.getvalue(),b'*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n*1\r\n$4\r\nPING\r\n')

    def test_invalid_or_unsupported_urls_fail_before_any_process_or_network(self):
        for suffix in ('/0?db=1&db=2','/0?db=1&%64b=2','/0?db=','/0?db',
                       '/0?db=-1','/0?db=+1','/0?db=1.0','/0?db=%ZZ',
                       '/0?db=1;db=2','/0?db=1&','/0?db=1&read_timeout=1',
                       '/0?protocol=3','/0?DB=1','/0?db=9223372036854775808',
                       '/invalid?db=1','/0/1?db=1','/-1','/1.0','/0#db=1'):
            with self.subTest(suffix=suffix),patch.dict(os.environ,{'RELAYHUB_DOCS_TEST_REDIS_URL':'redis://user:SENTINEL_URL_SECRET@redis.invalid:6379'+suffix}),patch.object(checker,'run',side_effect=AssertionError('process started before URL validation')),patch.object(checker.socket,'create_connection',side_effect=AssertionError('network before URL validation')):
                with self.assertRaisesRegex(AssertionError,'invalid docs-test Redis URL') as error:
                    with checker.runtime():pass
                self.assertNotIn('SENTINEL_URL_SECRET',str(error.exception))

class ExternalRedisRuntime(unittest.TestCase):
    def test_query_database_state_cleaned_and_other_databases_untouched(self):
        self.assertTrue(os.getenv('RELAYHUB_DOCS_TEST_REDIS_URL'),'explicit docs Redis service is required')
        url=urllib.parse.urlsplit(os.environ['RELAYHUB_DOCS_TEST_REDIS_URL'])._replace(path='/0',query='db=1').geturl()
        sentinel='unrelated-docs-db-fixture-'+secrets.token_hex(12)
        prefixes=[];real_popen=checker.subprocess.Popen
        def popen(args,*a,**kw):
            if str(args[0]).endswith('/relayhub'):prefixes.append(kw['env']['RELAYHUB_REDIS_KEY_PREFIX'])
            return real_popen(args,*a,**kw)
        for database in (0,1):redis('SET',sentinel,'preserve-db-'+str(database),database=database)
        try:
            with patch.dict(os.environ,{'RELAYHUB_DOCS_TEST_REDIS_URL':url}),patch.object(checker.subprocess,'Popen',side_effect=popen):
                for fail in (False,True):
                    try:
                        with checker.runtime() as (base,admin):
                            status,_,_=checker.request(base,'/api/v1/apps','POST',json.dumps({'name':'docs-db-isolation','delivery_mode':'queue'}).encode(),{'Authorization':'Bearer '+admin,'Content-Type':'application/json'})
                            self.assertEqual(status,201)
                            self.assertTrue(redis('KEYS',prefixes[-1]+':*',database=1),'app state must use query-selected DB 1')
                            self.assertEqual(redis('KEYS',prefixes[-1]+':*',database=0),[],'app state must not use path DB 0')
                            if fail:raise RuntimeError('intentional fixture failure')
                    except RuntimeError:
                        if not fail:raise
                    self.assertEqual(len(redis('KEYS',prefixes[-1]+':*',database=1)),0,'generated credentials/state leaked in effective DB 1')
                    for database in (0,1):
                        self.assertEqual(redis('GET',sentinel,database=database),('preserve-db-'+str(database)).encode())
            self.assertEqual(len(prefixes),2);self.assertNotEqual(*prefixes)
        finally:
            # Keep the failing regression safe too: delete only this fixture's keys.
            for database in (0,1):
                for prefix in prefixes:
                    keys=redis('KEYS',prefix+':*',database=database)
                    if keys:redis('DEL',*keys,database=database)
                redis('DEL',sentinel,database=database)

    def test_external_service_without_host_binary_isolated_and_cleaned(self):
        self.assertTrue(os.getenv('RELAYHUB_DOCS_TEST_REDIS_URL'),'explicit docs Redis service is required')
        sentinel='unrelated-docs-fixture-'+secrets.token_hex(12)
        redis('SET',sentinel,'preserve-me')
        prefixes=[];real_which=checker.shutil.which;real_popen=checker.subprocess.Popen
        def popen(args,*a,**kw):
            self.assertNotEqual(args[0],'redis-server','must use supplied external Redis')
            if str(args[0]).endswith('/relayhub'):prefixes.append(kw['env']['RELAYHUB_REDIS_KEY_PREFIX'])
            return real_popen(args,*a,**kw)
        try:
            with patch.object(checker.shutil,'which',side_effect=lambda name:None if name=='redis-server' else real_which(name)),patch.object(checker.subprocess,'Popen',side_effect=popen):
                for fail in (False,True):
                    try:
                        with checker.runtime() as (base,admin):
                            status,_,_=checker.request(base,'/api/v1/apps','POST',json.dumps({'name':'docs-isolation','delivery_mode':'queue'}).encode(),{'Authorization':'Bearer '+admin,'Content-Type':'application/json'})
                            self.assertEqual(status,201)
                            self.assertTrue(redis('KEYS',prefixes[-1]+':*'))
                            if fail:raise RuntimeError('intentional fixture failure')
                    except RuntimeError:
                        if not fail:raise
                    self.assertEqual(redis('KEYS',prefixes[-1]+':*'),[],'owned keys must be removed on success/failure')
                    self.assertEqual(redis('GET',sentinel),b'preserve-me','unrelated Redis state was deleted')
            self.assertEqual(len(prefixes),2);self.assertNotEqual(*prefixes)
        finally:redis('DEL',sentinel)

    def test_runtime_refuses_missing_dependency(self):
        with patch.dict(os.environ,{'RELAYHUB_DOCS_TEST_REDIS_URL':''}),patch.object(checker.shutil,'which',return_value=None):
            with self.assertRaisesRegex(AssertionError,'Redis|redis'):
                with checker.runtime():pass

if __name__=='__main__':unittest.main()

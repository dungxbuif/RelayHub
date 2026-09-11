#!/usr/bin/env python3
"""Real external-Redis runtime isolation checks; no host Redis binary is used."""
import importlib.util, json, os, secrets, socket, urllib.parse, unittest
from pathlib import Path
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('check_docs',Path(__file__).with_name('check-docs.py'))
checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)

def redis(*parts):
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
        if url.path.strip('/'):command(['SELECT',url.path.strip('/')])
        return command(parts)

class ExternalRedisRuntime(unittest.TestCase):
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

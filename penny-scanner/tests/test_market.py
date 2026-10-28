import asyncio
import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime,timezone
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from market import Alpaca, scan_market
from scanner import Bar, Engine
from worker import Config, Outbox, sender


class MarketTests(unittest.IsolatedAsyncioTestCase):
    async def test_paginated_history_covers_all_symbols(self):
        class Fake(Alpaca):
            def __init__(self):self.calls=[]
            async def get(self,path,params,catalog=False):
                self.calls.append(params)
                item={'t':'2026-10-28T08:00:00Z','o':2,'h':2.1,'l':1.9,'c':2,'v':100,'vw':2}
                if len(self.calls)==1:return {'bars':{'A':[item]},'next_page_token':'page2'}
                return {'bars':{'B':[item]},'next_page_token':None}
        api=Fake()
        with patch('market.datetime') as clock:
            clock.now.side_effect=lambda tz:datetime(2026,10,28,14,50,tzinfo=timezone.utc).astimezone(tz)
            data=await api.history(['A','B'])
        self.assertEqual(len(data['A']),1)
        self.assertEqual(len(data['B']),1)
        self.assertEqual(api.calls[1]['page_token'],'page2')

    async def test_signal_pipeline_from_live_fixture_to_outbox_to_sender(self):
        start=int(datetime.now(timezone.utc).timestamp()//300)*300-30*60
        history=[Bar(start+i*60,2,2.02,1.99,2,20000,2) for i in range(30)]
        stamp=datetime.fromtimestamp(start+30*60,timezone.utc).isoformat()
        sent=[]
        stop=asyncio.Event()
        async def server_handler(ws):
            await ws.send(json.dumps([{'T':'success','msg':'authenticated'}]))
            sub=json.loads(await ws.recv())
            self.assertEqual(sub['bars'],['TEST'])
            self.assertEqual(sub['quotes'],['TEST'])
            # To pass freshness validation, the fixture represents the most recent closed minute.
            t=datetime.now(timezone.utc).isoformat()
            bt=datetime.fromtimestamp(int(datetime.now(timezone.utc).timestamp()//60)*60-60,timezone.utc).isoformat()
            # Shift the seed to immediately precede this last completed minute.
            await ws.send(json.dumps([{'T':'q','S':'TEST','bp':2.099,'ap':2.101,'t':t},
                                      {'T':'b','S':'TEST','t':bt,'o':2,'h':2.11,'l':2,'c':2.1,'v':80000,'vw':2.07}]))
            await stop.wait()
        async with serve(server_handler,'127.0.0.1',0) as server:
            ws_url=f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}'
            class FakeMarket:
                headers={}
                async def universe(self):return ['TEST']
                async def discover(self,*args):return [('TEST',10,600000,1.8)]
                async def history(self,symbols):
                    base=int(datetime.now(timezone.utc).timestamp()//60)*60-31*60
                    return {'TEST':[Bar(base+i*60,2,2.02,1.99,2,20000,2) for i in range(30)]}
            class FakeTelegram:
                async def send(self,cfg,text):
                    sent.append(text)
                    stop.set()
                    return 8
            def connector(uri,**kwargs):
                return connect(ws_url,**kwargs)
            with tempfile.TemporaryDirectory() as tmp:
                box=Outbox(str(Path(tmp)/'db'))
                engine=Engine()
                producer=asyncio.create_task(scan_market(engine,box,stop,client=FakeMarket(),connector=connector))
                consumer=asyncio.create_task(sender(FakeTelegram(),Config('k','b','t','123'),box,stop))
                try:
                    await asyncio.wait_for(stop.wait(),4)
                    self.assertTrue(any('زخم نشط' in text for text in sent))
                    self.assertEqual(box.db.execute("SELECT state FROM alerts").fetchone()[0],'sent')
                finally:
                    stop.set(); producer.cancel()
                    await asyncio.gather(producer,consumer,return_exceptions=True)
                    box.db.close()

    async def test_expired_signal_is_not_sent_as_current(self):
        import time
        class Sink:
            attempts=0
            async def send(self,cfg,text):self.attempts+=1; return 1
        with tempfile.TemporaryDirectory() as tmp:
            box=Outbox(str(Path(tmp)/'db'))
            box.enqueue(('expired','example',time.time()-90),ttl=60)
            stop=asyncio.Event(); sink=Sink()
            task=asyncio.create_task(sender(sink,Config('k','b','t','1'),box,stop))
            await asyncio.sleep(.05)
            stop.set();await task
            self.assertEqual(sink.attempts,0)
            self.assertEqual(box.db.execute('SELECT state FROM alerts').fetchone()[0],'expired')
            box.db.close()


if __name__=='__main__':unittest.main()

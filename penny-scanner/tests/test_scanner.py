import copy
import json
import tempfile
import unittest
from datetime import datetime,timezone
from pathlib import Path

from scanner import Bar, Engine, Rules, five_minute, format_signal

START=datetime(2026,10,28,14,0,tzinfo=timezone.utc).timestamp()


def flat(n=30):
    return [Bar(START+i*60,2,2.02,1.99,2,20000,2) for i in range(n)]


def ready(bars, rules=None):
    engine=Engine(rules)
    engine.set_symbols(['TEST'])
    engine.seed('TEST',bars,1.8)
    return engine


def push(engine,bar,age=0,spread=.002,correction=False):
    now=bar.t+61
    engine.quote('TEST',now-age,bar.c-spread/2,bar.c+spread/2)
    return engine.on_bar('TEST',bar,now=now,correction=correction)


class SignalTests(unittest.TestCase):
    def test_breakout_requires_future_retest_not_just_high_print(self):
        bars=flat(25)
        engine=ready(bars)
        kinds=[]
        for i in range(25,30):
            b=Bar(START+i*60,2.03,2.10,2.025,2.08,60000,2.06)
            kinds.extend(s['kind'] for s in push(engine,b))
        self.assertNotIn('breakout_confirmed',kinds)
        self.assertIsNotNone(engine.states['TEST'].arm)
        retest=Bar(START+30*60,2.025,2.075,2.018,2.06,40000,2.04)
        signals=push(engine,retest)
        breakout=[s for s in signals if s['kind']=='breakout_confirmed']
        self.assertEqual(len(breakout),1)
        s=breakout[0]
        self.assertLess(s['stop'],s['entry'])
        self.assertAlmostEqual(s['targets'][1]-s['entry'],2*(s['entry']-s['stop']))
        self.assertEqual(push(engine,retest),[])

    def test_failed_retest_never_confirms(self):
        engine=ready(flat(30))
        engine.states['TEST'].arm={'level':2.02,'t':START+29*60}
        bar=Bar(START+30*60,2.04,2.05,1.98,1.99,70000,2.0)
        self.assertNotIn('breakout_confirmed',[s['kind'] for s in push(engine,bar)])
        self.assertIsNone(engine.states['TEST'].arm)

    def test_failed_level_cancels_even_with_no_current_quote(self):
        engine=ready(flat(30))
        engine.states['TEST'].arm={'level':2.02,'t':START+29*60}
        bar=Bar(START+30*60,2.04,2.05,1.98,1.99,70000,2)
        self.assertEqual(engine.on_bar('TEST',bar,now=bar.t+61),[])
        self.assertIsNone(engine.states['TEST'].arm)

    def test_momentum_and_cooldown(self):
        engine=ready(flat(30))
        bar=Bar(START+30*60,2,2.11,2,2.10,80000,2.07)
        signals=push(engine,bar)
        self.assertIn('momentum',[s['kind'] for s in signals])
        second=Bar(START+31*60,2.10,2.16,2.09,2.15,100000,2.13)
        self.assertNotIn('momentum',[s['kind'] for s in push(engine,second)])

    def test_accumulation_is_labeled_as_proxy(self):
        bars=[]
        for i in range(30):
            if i<20:
                bars.append(Bar(START+i*60,2,2.04,1.97,2,20000,2))
            else:
                c=2+(i-19)*.001
                bars.append(Bar(START+i*60,c-.0005,c+.004,c-.004,c,20000,c))
        engine=ready(bars[:-1])
        signals=push(engine,bars[-1])
        found=[s for s in signals if s['kind']=='accumulation_candidate']
        self.assertTrue(found)
        self.assertIn('تجميع محتمل',format_signal(found[0]))
        self.assertIn('للمراقبة فقط',format_signal(found[0]))

    def test_support_rejection_bounce(self):
        engine=ready(flat(30))
        bar=Bar(START+30*60,2.02,2.08,1.99,2.04,60000,2.02)
        self.assertIn('bounce_candidate',[s['kind'] for s in push(engine,bar)])

    def test_stale_quote_spread_and_gap_suppress_signals(self):
        bar=Bar(START+30*60,2,2.11,2,2.1,80000,2.07)
        self.assertEqual(push(ready(flat(30)),bar,age=20),[])
        self.assertEqual(push(ready(flat(30)),bar,spread=.10),[])
        bars=flat(30)
        del bars[-5]
        self.assertEqual(push(ready(bars),bar),[])

    def test_halt_price_bound_and_no_future_data(self):
        bar=Bar(START+30*60,2,2.11,2,2.1,80000,2.07)
        e=ready(flat(30)); e.states['TEST'].halted=True
        self.assertEqual(push(e,bar),[])
        e=ready(flat(30),Rules(max_price=2.05))
        self.assertEqual(push(e,bar),[])
        e=ready(flat(30))
        self.assertEqual(push(e,bar,age=-10),[])

    def test_corrections_update_state_without_retroactive_signal(self):
        e=ready(flat(30))
        bar=Bar(START+29*60,2,2.11,2,2.1,80000,2.07)
        self.assertEqual(push(e,bar,correction=True),[])
        self.assertEqual(e.states['TEST'].bars[bar.t].c,2.1)

    def test_strict_metadata_and_news_gate_missing_data(self):
        bar=Bar(START+30*60,2,2.11,2,2.1,80000,2.07)
        self.assertEqual(push(ready(flat(30),Rules(require_news=True)),bar),[])
        self.assertEqual(push(ready(flat(30),Rules(require_verified_metadata=True)),bar),[])

    def test_warmup_and_day_reset_do_not_use_yesterdays_vwap(self):
        e=ready(flat(30))
        self.assertEqual(push(e,flat(30)[-1]),[])
        nxt=Bar(START+86400,2,2.11,2,2.1,80000,2.07)
        self.assertEqual(push(e,nxt),[])
        self.assertEqual(len(e.states['TEST'].bars),1)
        self.assertEqual(e.states['TEST'].previous_close,0)

    def test_missing_minute_does_not_form_five_minute_bar(self):
        bars=flat(5)
        self.assertEqual(len(five_minute(bars)),1)
        self.assertEqual(five_minute(bars[:2]+bars[3:]),[])

    def test_risk_quantity_has_cash_cap(self):
        e=ready(flat(30),Rules(capital=10000))
        bar=Bar(START+30*60,2,2.11,2,2.1,80000,2.07)
        s=[s for s in push(e,bar) if s['kind']=='momentum'][0]
        self.assertLessEqual(s['quantity']*s['entry'],10000)
        self.assertLessEqual(s['quantity']*(s['entry']-s['stop']),100)

    def test_invalid_strategy_does_not_silently_run(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'strategy.json'
            path.write_text(json.dumps({'capital':-1}))
            with self.assertRaises(ValueError):Rules.load(path)
            path.write_text(json.dumps({'require_news':'false'}))
            with self.assertRaises(ValueError):Rules.load(path)


if __name__=='__main__':unittest.main()

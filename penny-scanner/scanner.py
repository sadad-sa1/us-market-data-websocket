"""Long-only intraday signal rules. Technical confirmation is not certainty."""
import hashlib
import json
import math
from urllib.parse import urlsplit
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")


def timestamp(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def session_day(stamp):
    return datetime.fromtimestamp(stamp, NY).date()


@dataclass(frozen=True)
class Bar:
    t: float
    o: float
    h: float
    l: float
    c: float
    v: float
    vw: float

    @classmethod
    def parse(cls, item):
        bar = cls(timestamp(item["t"]), *(float(item[x]) for x in ("o", "h", "l", "c", "v", "vw")))
        if not all(math.isfinite(x) for x in (bar.t, bar.o, bar.h, bar.l, bar.c, bar.v, bar.vw)):
            raise ValueError("Nonfinite bar")
        if min(bar.o, bar.h, bar.l, bar.c, bar.vw) <= 0 or bar.v < 0 or bar.l > min(bar.o, bar.c) or bar.h < max(bar.o, bar.c):
            raise ValueError("Invalid OHLCV")
        return bar


@dataclass
class Rules:
    min_price: float = 0.1
    max_price: float = 5.0
    min_day_volume: int = 500000
    min_change_pct: float = 5.0
    max_spread_pct: float = 2.0
    quote_age: float = 15.0
    bar_age: float = 120.0
    breakout_volume_ratio: float = 2.0
    momentum_volume_ratio: float = 2.0
    momentum_5m_pct: float = 3.0
    breakout_buffer_pct: float = 0.15
    retest_tolerance_pct: float = 0.5
    retest_expiry_minutes: int = 15
    cooldown_minutes: int = 15
    max_stop_pct: float = 8.0
    news_age_hours: int = 72
    require_news: bool = False
    require_verified_metadata: bool = False
    capital: float = 0.0
    risk_pct: float = 1.0

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        unknown = set(data) - set(cls.__dataclass_fields__)
        if unknown:
            raise ValueError("Unknown strategy fields: " + ",".join(sorted(unknown)))
        result = cls(**data)
        for name in cls.__dataclass_fields__:
            value = getattr(result, name)
            if name.startswith("require_"):
                if not isinstance(value, bool):
                    raise ValueError("Boolean required")
            elif isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("Invalid strategy value: " + name)
        if result.min_price >= result.max_price or result.risk_pct > 2 or result.risk_pct == 0:
            raise ValueError("Invalid price bounds or risk percentage")
        return result


@dataclass
class State:
    bars: dict = field(default_factory=dict)
    quote: tuple | None = None  # timestamp, bid, ask
    previous_close: float = 0
    metadata: dict = field(default_factory=dict)
    news: dict | None = None
    halted: bool = False
    arm: dict | None = None
    cooldown: dict = field(default_factory=dict)
    last_evaluated: float = 0

    def ordered(self):
        return [self.bars[k] for k in sorted(self.bars)]


def five_minute(bars):
    groups = defaultdict(list)
    for bar in bars:
        groups[int(bar.t // 300)].append(bar)
    result = []
    for group in groups.values():
        group.sort(key=lambda b: b.t)
        if len(group) != 5 or group[0].t % 300 != 0 or any(b.t - a.t != 60 for a,b in zip(group, group[1:])):
            continue
        vol = sum(b.v for b in group)
        result.append(Bar(group[0].t, group[0].o, max(b.h for b in group), min(b.l for b in group),
                          group[-1].c, vol, sum(b.v*b.vw for b in group)/vol if vol else group[-1].c))
    return result


class Engine:
    def __init__(self, rules=None):
        self.rules = rules or Rules()
        self.states = {}

    def set_symbols(self, symbols):
        # Keep history for active candidates only, to bound RAM.
        self.states = {s:self.states.get(s, State()) for s in symbols}

    def seed(self, symbol, bars, previous_close, metadata=None):
        state = self.states.setdefault(symbol, State())
        state.previous_close = float(previous_close)
        state.metadata = metadata or state.metadata
        if not bars:
            return
        day = session_day(max(b.t for b in bars))
        state.bars = {b.t:b for b in bars if session_day(b.t) == day}
        # Historical warm-up must never trigger a live signal.
        state.last_evaluated = max(state.bars, default=0)
        state.arm = None

    def quote(self, symbol, t, bid, ask):
        state = self.states.get(symbol)
        if state and all(math.isfinite(x) for x in (t,bid,ask)) and bid > 0 and ask >= bid:
            if not state.quote or t >= state.quote[0]:
                state.quote = (t,bid,ask)

    def add_news(self, event):
        data = event.get("data", {})
        content = data.get("content", {})
        if not isinstance(content, dict):
            return False
        try:
            from worker import parse_date
            t = parse_date(content.get("updated") or data.get("timestamp") or content.get("created")).timestamp()
        except (TypeError, ValueError, AttributeError):
            return False
        matched = False
        for stock in content.get("stocks", []):
            symbol = stock.get("name", "").upper() if isinstance(stock, dict) else ""
            state = self.states.get(symbol)
            if state:
                matched = True
                if str(data.get("action", "")).lower() == "deleted":
                    if state.news and state.news.get("id") == content.get("id"):
                        state.news = None
                elif not state.news or t >= state.news["t"]:
                    state.news = {"t":t, "title":str(content.get("title", ""))[:250],
                                  "url":str(content.get("url", ""))[:600], "id":content.get("id")}
        return matched

    def _verified(self, state, now):
        m = state.metadata
        try:
            age = now - timestamp(m["updated_at"])
            source = urlsplit(m.get('source_url',''))
            return (0 <= age <= 72*3600 and 0 < float(m["float"]) < 50_000_000
                    and float(m["market_cap"]) > 20_000_000 and m.get("dilution_review") == "clear"
                    and m.get("reverse_split_review") == "clear" and source.scheme=='https' and bool(source.hostname))
        except (ValueError, KeyError, TypeError):
            return False

    def on_bar(self, symbol, bar, now=None, correction=False):
        now = now if now is not None else datetime.now(timezone.utc).timestamp()
        state = self.states.get(symbol)
        if not state:
            return []
        history = state.ordered()
        if history and session_day(bar.t) < session_day(history[-1].t):
            return []
        if history and session_day(bar.t) != session_day(history[-1].t):
            # Do not carry yesterday's VWAP, arms, news qualification or close into a new session.
            state.bars.clear()
            state.arm = None
            state.previous_close = 0  # refreshed by discovery snapshots
            state.cooldown.clear()
            state.last_evaluated = 0
        state.bars[bar.t] = bar
        if correction or bar.t <= state.last_evaluated:
            # Revised candles update indicators but do not retroactively issue signals.
            state.arm = None
            return []
        state.last_evaluated = bar.t
        bars = state.ordered()
        r = self.rules
        if state.arm and (bar.t-state.arm['t'] > r.retest_expiry_minutes*60 or
                          bar.c < state.arm['level']*(1-r.retest_tolerance_pct/100)):
            # Structural failure cancels even when liquidity/quote gates fail this minute.
            state.arm = None
        if len(bars) < 26 or state.halted or not state.quote or state.previous_close <= 0:
            return []
        if any(b.t-a.t != 60 for a,b in zip(bars[-26:],bars[-25:])):
            state.arm = None
            return []
        qt,bid,ask = state.quote
        spread = (ask-bid)/((ask+bid)/2)*100
        # A minute bar's timestamp is its START; use end time for freshness.
        if not 0 <= now-qt <= r.quote_age or not 0 <= now-(bar.t+60) <= r.bar_age:
            return []
        if not (r.min_price <= bar.c < r.max_price) or spread > r.max_spread_pct:
            return []
        if ask > bar.c*1.01 or bid < bar.c*0.99:  # price has already moved; avoid stale/chased triggers
            return []
        volume = sum(b.v for b in bars)
        change = (bar.c/state.previous_close-1)*100
        if volume < r.min_day_volume or change < r.min_change_pct:
            return []
        vwap = sum(b.v*b.vw for b in bars)/volume if volume else 0
        baseline = mean(b.v for b in bars[-21:-1])
        ratio = bar.v/baseline if baseline else 0
        atr = mean(max(b.h-b.l,abs(b.h-a.c),abs(b.l-a.c)) for a,b in zip(bars[-15:-1],bars[-14:]))
        verified = self._verified(state, now)
        news = state.news if state.news and 0 <= now-state.news['t'] <= r.news_age_hours*3600 else None
        if (r.require_news and not news) or (r.require_verified_metadata and not verified):
            return []
        ctx = dict(volume=volume,change=change,vwap=vwap,volume_ratio=ratio,spread=spread,
                   atr=atr,verified=verified,news=news)
        setups = []
        # 5m close above the prior four complete 5m candles, then a SUBSEQUENT 1m retest.
        completed = five_minute(bars)
        arm = state.arm
        if arm:
            level = arm['level']
            if bar.t-arm['t'] > r.retest_expiry_minutes*60 or bar.c < level*(1-r.retest_tolerance_pct/100):
                state.arm = None
            elif bar.t > arm['t'] and bar.l <= level*(1+r.retest_tolerance_pct/100) and bar.l >= level*(1-r.retest_tolerance_pct/100) and bar.c > level and bar.c > bar.o and bar.c > vwap:
                setups.append(('breakout_confirmed',level,min(bar.l,level)-max(atr*0.1,bar.c*0.001)))
                state.arm = None
        if len(completed) >= 5 and completed[-1].t+240 == bar.t:
            recent = completed[-1]
            prior = completed[-5:-1]
            contiguous = all(b.t-a.t == 300 for a,b in zip(completed[-5:],completed[-4:]))
            resistance = max(b.h for b in prior)
            prior_vol = mean(b.v for b in prior)
            if contiguous and recent.c > resistance*(1+r.breakout_buffer_pct/100) and recent.v >= prior_vol*r.breakout_volume_ratio and recent.c > vwap:
                state.arm = {'level':resistance,'t':bar.t}
        # Minute-close momentum, measured without using an unfinished candle.
        five_change = (bar.c/bars[-6].c-1)*100
        if five_change >= r.momentum_5m_pct and ratio >= r.momentum_volume_ratio and bar.c > vwap and bar.c > max(b.h for b in bars[-6:-1]):
            setups.append(('momentum',bar.c,min(b.l for b in bars[-3:])-atr*0.1))
        # Price/volume accumulation PROXY, never a statement about institutional buying.
        window = bars[-10:]
        width = (max(b.h for b in window)-min(b.l for b in window))/bar.c*100
        signed = sum(b.v if b.c>a.c else -b.v if b.c<a.c else 0 for a,b in zip(bars[-11:-1],window))
        total = sum(b.v for b in window)
        recent_range = mean(b.h-b.l for b in window[-5:])
        old_range = mean(b.h-b.l for b in bars[-20:-10])
        if width <= 3 and total and signed/total >= .25 and recent_range <= old_range*.8 and min(b.l for b in window[-3:]) > min(b.l for b in window[:3]) and bar.c >= vwap:
            setups.append(('accumulation_candidate',max(b.h for b in window),min(b.l for b in window)-atr*.1))
        # Rejection at a prior support or reclaimed VWAP, followed by green close above previous high.
        support = min(b.l for b in bars[-21:-1])
        near_support = abs(bar.l-support)/support <= .007
        reclaim_vwap = bar.l <= vwap <= bar.c
        wick = min(bar.o,bar.c)-bar.l
        if (near_support or reclaim_vwap) and bar.c>bars[-2].h and bar.c>bar.o and wick >= .3*(bar.h-bar.l) and ratio>=1.5:
            setups.append(('bounce_candidate',bar.c,bar.l-max(atr*.1,bar.c*.001)))
        signals = []
        for kind, level, stop in setups:
            if bar.t-state.cooldown.get(kind,0) < r.cooldown_minutes*60:
                continue
            entry = max(level,ask)
            # Accumulation is a watch alert with a conditional trigger, never a current entry.
            risk = entry-stop
            if risk <= 0 or risk/entry*100>r.max_stop_pct:
                continue
            state.cooldown[kind] = bar.t
            key = hashlib.sha256(f'{symbol}:{kind}:{bar.t}'.encode()).hexdigest()
            signals.append(dict(key=key,symbol=symbol,kind=kind,t=bar.t+60,entry=entry,stop=stop,
                                targets=[entry+risk*x for x in (1,2,3)],level=level,
                                quantity=min(int(r.capital*r.risk_pct/100/risk),int(r.capital/entry)) if r.capital else None,
                                **ctx))
        return signals


def format_signal(s):
    label = {'breakout_confirmed':'اختراق مؤكد بالشروط الفنية', 'momentum':'زخم نشط',
             'accumulation_candidate':'تجميع محتمل — للمراقبة', 'bounce_candidate':'ارتداد مرشح'}[s['kind']]
    p = lambda x: f'{x:.4f}'.rstrip('0').rstrip('.')
    when = datetime.fromtimestamp(s['t'],ZoneInfo('Asia/Riyadh')).strftime('%Y-%m-%d %H:%M:%S')
    news = s['news']
    news_text = f"{news['title']}\n{news['url']}" if news else 'لا يوجد خبر محفوظ حديث للسهم منذ بدء الاستماع؛ ليس نفيًا لوجود أخبار.'
    status = 'مراقبة؛ يلزم تجاوز المستوى أولًا' if s['kind']=='accumulation_candidate' else 'إشارة فنية عند الإغلاق؛ راجع السعر والسبريد قبل التنفيذ'
    if not s['verified']:
        status += '\nالفلوت والقيمة السوقية والتخفيف والتقسيم العكسي غير معتمدة؛ للمراقبة فقط.'
    return (f"{s['symbol']} | {label}\nوقت الإشارة: {when} السعودية\n{status}\n\n"
            f"التغير: {s['change']:+.2f}% | حجم الجلسة الممتدة: {int(s['volume']):,}\n"
            f"حجم الدقيقة/متوسط 20 دقيقة: {s['volume_ratio']:.2f}x\n"
            f"VWAP من 04:00 نيويورك: {p(s['vwap'])} | السبريد: {s['spread']:.2f}%\n"
            f"مستوى التفعيل: {p(s['entry'])} | الإبطال/الوقف النظري: {p(s['stop'])}\n"
            f"أهداف نظرية 1R / 2R / 3R: {' / '.join(p(x) for x in s['targets'])}\n"
            f"الحجم وفق رأس المال: {s['quantity'] if s['quantity'] is not None else 'غير محسوب؛ رأس المال غير محدد'}\n"
            f"الإلغاء: فقد المستوى أو VWAP، اتساع السبريد أو تقادم البيانات.\n\n"
            f"خبر Benzinga المحفوظ:\n{news_text}\n\n"
            'تأكيد الشروط لا يضمن استمرار الحركة. لا ينفذ البرنامج صفقات.')

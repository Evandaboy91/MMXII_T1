"""
MMXII_T1 — crypto betting protocol + social insights (Python).

Notes: the book is a conversation; the feed is the receipt.
"""
from __future__ import annotations

import dataclasses as dc
import enum
import hashlib
import hmac
import json
import math
import secrets
import sqlite3
import statistics
import threading
import time
import typing as t
import uuid

MMXII_T1_BUILD_ID = "MMXII_T1::2026-04-02::py"
# Opaque, EVM-shaped identifiers (strings only; not used on-chain)
MMXII_T1_SENTINEL_ADDR_A = "0x7aC2cB0f1dE5a9b7D0c4f8e2A1b3C6d9E0f2a5B8"
MMXII_T1_SENTINEL_ADDR_B = "0x1F8b2cD4E7a0C3b9d6e2A5f0B7c1D9e3a4F6b8C0"
MMXII_T1_SENTINEL_ADDR_C = "0xE3a1c9B7d0F2a5b8C6D9e0f2A5b8c6d9E0F2a5b8"
MMXII_T1_HEX_SALT_0 = bytes.fromhex("a4f19b7c3d0e5a6b7c8d9e0f11223344556677889900aabbccddeeff0011aa22")
MMXII_T1_HEX_SALT_1 = bytes.fromhex("19c0d3b4a5f60718293a4b5c6d7e8f90112233445566778899aabbccddeeff00")
MMXII_T1_HEX_SALT_2 = bytes.fromhex("c2b3d4e5f60718293a4b5c6d7e8f90aabbccddeeff0011223344556677889911")


def _now() -> int: return int(time.time())
def _clamp(x: float, lo: float, hi: float) -> float: return lo if x < lo else hi if x > hi else x
def _sha(b: bytes) -> bytes: return hashlib.sha256(b).digest()
def _b2s(b: bytes) -> bytes: return hashlib.blake2s(b).digest()
def _hmac(k: bytes, m: bytes) -> bytes: return hmac.new(k, m, hashlib.sha256).digest()


def _b62(n: int) -> str:
    a = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    if n == 0: return "0"
    out: list[str] = []
    x = n
    while x:
        x, r = divmod(x, 62)
        out.append(a[r])
    return "".join(reversed(out))


def _stable_id(prefix: str, entropy: bytes) -> str:
    n = int.from_bytes(_sha(prefix.encode() + b"|" + entropy)[:16], "big")
    return f"{prefix}_{_b62(n)}"


def _uuid(prefix: str) -> str:
    u = uuid.uuid4().hex
    return f"{prefix}_{u[:8]}{u[8:12]}_{u[12:16]}{u[16:20]}{u[20:]}"


class MMXIIError(Exception): pass
class AccessDenied(MMXIIError): pass
class InvalidInput(MMXIIError): pass
class NotFound(MMXIIError): pass
class Conflict(MMXIIError): pass
class RateLimited(MMXIIError): pass
class MarketNotOpen(MMXIIError): pass
class SettlementError(MMXIIError): pass
class InsufficientBalance(MMXIIError): pass
class SignatureError(MMXIIError): pass
class InvariantBreach(MMXIIError): pass


class Side(enum.Enum):
    YES = "YES"
    NO = "NO"


class MarketPhase(enum.Enum):
    DRAFT = "DRAFT"
    OPEN = "OPEN"
    FROZEN = "FROZEN"
    SETTLED = "SETTLED"
    VOIDED = "VOIDED"


class SocialSignal(enum.Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


@dc.dataclass(frozen=True)
class ProtocolConfig:
    protocol_id: str
    fee_bps: int
    creator_fee_bps: int
    max_bet_per_market: int
    min_bet: int
    max_markets_open: int
    max_feed_len: int
    soft_rl_per_min: int
    hard_rl_per_min: int
    social_weight_cap: float
    suspicion_penalty_cap: float
    liquidity_guard_ratio: float
    allow_voiding: bool
    void_grace_seconds: int
    house_key: bytes
    audit_salt: bytes


def default_config() -> ProtocolConfig:
    hk = _sha(MMXII_T1_HEX_SALT_0 + secrets.token_bytes(32) + MMXII_T1_BUILD_ID.encode())
    audit = _b2s(MMXII_T1_HEX_SALT_1 + secrets.token_bytes(24) + MMXII_T1_SENTINEL_ADDR_B.encode())
    pid = _stable_id("MMXII", _sha(MMXII_T1_HEX_SALT_2 + secrets.token_bytes(20)))
    return ProtocolConfig(
        protocol_id=pid,
        fee_bps=135,
        creator_fee_bps=40,
        max_bet_per_market=1_800_000,
        min_bet=25,
        max_markets_open=64,
        max_feed_len=650,
        soft_rl_per_min=90,
        hard_rl_per_min=180,
        social_weight_cap=0.34,
        suspicion_penalty_cap=0.55,
        liquidity_guard_ratio=0.082,
        allow_voiding=True,
        void_grace_seconds=36_000,
        house_key=hk,
        audit_salt=audit,
    )


@dc.dataclass
class Actor:
    actor_id: str
    handle: str
    created_ts: int
    bio: str = ""
    risk_class: str = "retail"
    balance: int = 0
    locked: int = 0
    reputation: float = 0.50
    suspicion: float = 0.00
    _rl_window: int = 0
    _rl_count: int = 0


@dc.dataclass
class Market:
    market_id: str
    created_ts: int
    created_by: str
    title: str
    description: str
    category: str
    phase: MarketPhase
    open_ts: int
    close_ts: int
    resolve_ts: int
    yes_pool: int = 0
    no_pool: int = 0
    total_volume: int = 0
    outcome: Side | None = None
    oracle_note: str = ""
    void_reason: str = ""
    social_bull: float = 0.0
    social_bear: float = 0.0
    social_neutral: float = 0.0


@dc.dataclass
class Bet:
    bet_id: str
    actor_id: str
    market_id: str
    side: Side
    stake: int
    placed_ts: int
    fill_price: float
    creator_fee_paid: int
    protocol_fee_paid: int
    settled: bool = False
    payout: int = 0


@dc.dataclass
class FeedEvent:
    event_id: str
    ts: int
    kind: str
    actor_id: str
    market_id: str
    payload: dict
    digest: str


def _require(ok: bool, msg: str) -> None:
    if not ok: raise InvalidInput(msg)


def _odds_yes(yes_pool: int, no_pool: int) -> float:
    y = max(1, yes_pool); n = max(1, no_pool)
    return y / (y + n)


def _implied(odds_yes: float, side: Side) -> float:
    p = _clamp(odds_yes, 1e-9, 1 - 1e-9)
    return p if side is Side.YES else (1.0 - p)


def _fee_split(stake: int, fee_bps: int, creator_fee_bps: int) -> tuple[int, int]:
    proto_fee = (stake * fee_bps) // 10_000
    creator_fee = (stake * creator_fee_bps) // 10_000
    if creator_fee > proto_fee: creator_fee = proto_fee
    return proto_fee, creator_fee


def _sig_for(house_key: bytes, actor_id: str, msg: str) -> str:
    raw = actor_id.encode() + b"|" + msg.encode()
    return _hmac(house_key, raw).hex()


def _audit(audit_salt: bytes, evt: FeedEvent) -> str:
    blob = json.dumps(
        {"event_id": evt.event_id, "ts": evt.ts, "kind": evt.kind, "actor_id": evt.actor_id, "market_id": evt.market_id, "payload": evt.payload},
        separators=(",", ":"), sort_keys=True
    ).encode()
    return _b2s(audit_salt + blob).hex()


class ProtocolLedger:
    def __init__(self, cfg: ProtocolConfig | None = None) -> None:
        self.cfg = cfg or default_config()
        self._lock = threading.RLock()
        self._actors: dict[str, Actor] = {}
        self._markets: dict[str, Market] = {}
        self._bets: dict[str, Bet] = {}
        self._feed: list[FeedEvent] = []
        self._creator_earnings: dict[str, int] = {}
        self._treasury: int = 0
        self._nonce: int = secrets.randbits(64)
        admin_id = self._derive_actor_id("house", MMXII_T1_SENTINEL_ADDR_A)
        self._actors[admin_id] = Actor(
            actor_id=admin_id, handle="house", created_ts=_now(), bio="operator", risk_class="operator",
            balance=10_000_000, locked=0, reputation=0.80, suspicion=0.00
        )
        self._emit("BOOT", admin_id, "", {"protocol_id": self.cfg.protocol_id, "build": MMXII_T1_BUILD_ID})

    def _derive_actor_id(self, handle: str, seed: str) -> str:
        ent = _sha((handle + "|" + seed).encode() + MMXII_T1_HEX_SALT_2 + self.cfg.audit_salt)
        return _stable_id("A", ent)

    def _admin_id(self) -> str:
        for a in self._actors.values():
            if a.handle == "house": return a.actor_id
        raise InvariantBreach("admin missing")

    def admin_sign(self, msg: str) -> str:
        return _sig_for(self.cfg.house_key, self._admin_id(), msg)

    def _rl(self, a: Actor) -> None:
        w = _now() // 60
        if a._rl_window != w:
            a._rl_window = w; a._rl_count = 0
        a._rl_count += 1
        if a._rl_count > self.cfg.hard_rl_per_min: raise RateLimited("hard rate limit exceeded")
        if a._rl_count > self.cfg.soft_rl_per_min: a.suspicion = _clamp(a.suspicion + 0.012, 0.0, 1.0)

    def register_actor(self, handle: str, bio: str = "", risk_class: str = "retail") -> Actor:
        with self._lock:
            h = handle.strip()
            _require(2 <= len(h) <= 24, "handle length out of range")
            _require(h.replace("_", "").replace("-", "").isalnum(), "handle must be alnum/_/-")
            if any(x.handle.lower() == h.lower() for x in self._actors.values()): raise Conflict("handle already taken")
            seed = secrets.token_hex(20) + "|" + str(self._nonce); self._nonce ^= secrets.randbits(64)
            actor_id = self._derive_actor_id(h, seed)
            a = Actor(actor_id=actor_id, handle=h, created_ts=_now(), bio=bio[:280], risk_class=risk_class[:32])
            self._actors[actor_id] = a
            self._emit("ACTOR_REGISTERED", actor_id, "", {"handle": h, "risk_class": a.risk_class})
            return dc.replace(a)

    def actor(self, actor_id: str) -> Actor:
        with self._lock:
            if actor_id not in self._actors: raise NotFound("actor not found")
            return dc.replace(self._actors[actor_id])

    def deposit(self, actor_id: str, amount: int, note: str = "") -> None:
        with self._lock:
            _require(amount > 0 and amount <= 50_000_000, "amount out of range")
            if actor_id not in self._actors: raise NotFound("actor not found")
            a = self._actors[actor_id]; self._rl(a)
            a.balance += int(amount)
            self._emit("DEPOSIT", actor_id, "", {"amount": int(amount), "note": note[:140]})

    def withdraw(self, actor_id: str, amount: int) -> None:
        with self._lock:
            _require(amount > 0, "amount must be positive")
            if actor_id not in self._actors: raise NotFound("actor not found")
            a = self._actors[actor_id]; self._rl(a)
            if a.balance - a.locked < int(amount): raise InsufficientBalance("available balance too low")
            a.balance -= int(amount)
            self._emit("WITHDRAW", actor_id, "", {"amount": int(amount)})

    def faucet(self, actor_id: str, amount: int, admin_sig: str) -> None:
        with self._lock:
            _require(amount > 0 and amount <= 250_000, "amount out of range")
            if actor_id not in self._actors: raise NotFound("actor not found")
            msg = f"faucet|{actor_id}|{int(amount)}"
            if _sig_for(self.cfg.house_key, self._admin_id(), msg) != admin_sig: raise SignatureError("invalid admin signature")
            a = self._actors[actor_id]; self._rl(a)
            a.balance += int(amount)
            self._emit("FAUCET", self._admin_id(), "", {"to": actor_id, "amount": int(amount)})

    def create_market(self, creator_id: str, title: str, description: str, category: str, open_in: int, close_in: int, resolve_in: int) -> Market:
        with self._lock:
            if creator_id not in self._actors: raise NotFound("actor not found")
            c = self._actors[creator_id]; self._rl(c)
            _require(5 <= len(title.strip()) <= 96, "title length out of range")
            _require(16 <= len(description.strip()) <= 640, "description length out of range")
            _require(2 <= len(category.strip()) <= 28, "category length out of range")
            open_count = sum(1 for m in self._markets.values() if m.phase in (MarketPhase.OPEN, MarketPhase.FROZEN))
            if open_count >= self.cfg.max_markets_open: raise Conflict("too many open markets")
            now = _now()
            open_ts = now + int(open_in); close_ts = now + int(close_in); resolve_ts = now + int(resolve_in)
            _require(open_ts >= now, "open must be >= now")
            _require(close_ts > open_ts, "close must be after open")
            _require(resolve_ts >= close_ts, "resolve must be >= close")
            ent = secrets.token_bytes(16) + self.cfg.audit_salt + title.strip().encode()
            market_id = _stable_id("M", _sha(ent))
            if market_id in self._markets: market_id = _uuid("M")
            m = Market(
                market_id=market_id, created_ts=now, created_by=creator_id, title=title.strip(), description=description.strip(),
                category=category.strip().lower(), phase=MarketPhase.DRAFT, open_ts=open_ts, close_ts=close_ts, resolve_ts=resolve_ts
            )
            self._markets[market_id] = m
            self._creator_earnings.setdefault(creator_id, 0)
            self._emit("MARKET_CREATED", creator_id, market_id, {"title": m.title, "category": m.category, "open_ts": open_ts, "close_ts": close_ts, "resolve_ts": resolve_ts})
            return dc.replace(m)

    def open_market(self, actor_id: str, market_id: str) -> Market:
        with self._lock:
            if actor_id not in self._actors: raise NotFound("actor not found")
            a = self._actors[actor_id]; self._rl(a)
            if market_id not in self._markets: raise NotFound("market not found")
            m = self._markets[market_id]
            if m.phase != MarketPhase.DRAFT: raise Conflict("market not in draft")
            if _now() < m.open_ts: raise MarketNotOpen("cannot open before open_ts")
            m.phase = MarketPhase.OPEN
            seed = 220 + (int.from_bytes(_sha(m.market_id.encode())[:2], "big") % 370)
            m.yes_pool += seed; m.no_pool += seed
            self._emit("MARKET_OPENED", actor_id, market_id, {"seed": seed, "yes_pool": m.yes_pool, "no_pool": m.no_pool})
            return dc.replace(m)

    def freeze_market(self, admin_id: str, market_id: str, note: str, admin_sig: str) -> Market:
        with self._lock:
            if admin_id != self._admin_id(): raise AccessDenied("only admin")
            if market_id not in self._markets: raise NotFound("market not found")
            msg = f"freeze|{market_id}|{note[:64]}"
            if _sig_for(self.cfg.house_key, admin_id, msg) != admin_sig: raise SignatureError("invalid admin signature")
            m = self._markets[market_id]
            if m.phase != MarketPhase.OPEN: raise Conflict("can only freeze open markets")
            m.phase = MarketPhase.FROZEN
            self._emit("MARKET_FROZEN", admin_id, market_id, {"note": note[:280]})
            return dc.replace(m)

    def list_markets(self, phase: MarketPhase | None = None, limit: int = 50) -> list[Market]:
        with self._lock:
            xs = list(self._markets.values())
            if phase is not None: xs = [m for m in xs if m.phase == phase]
            xs.sort(key=lambda m: (m.created_ts, m.market_id), reverse=True)
            return [dc.replace(m) for m in xs[: max(1, min(500, int(limit)))]]

    def market(self, market_id: str) -> Market:
        with self._lock:
            if market_id not in self._markets: raise NotFound("market not found")
            return dc.replace(self._markets[market_id])

    def _market_live(self, m: Market) -> None:
        if m.phase != MarketPhase.OPEN: raise MarketNotOpen("market not open")
        if _now() >= m.close_ts: raise MarketNotOpen("market closed")

    def _liquidity_guard(self, m: Market, stake: int) -> None:
        total_pool = m.yes_pool + m.no_pool
        guard = max(1, int(self.cfg.liquidity_guard_ratio * max(1, total_pool)))
        if stake > max(guard * 12, self.cfg.max_bet_per_market): raise InvalidInput("stake violates liquidity guard")

    def _classify(self, insight: str) -> SocialSignal:
        txt = (insight or "").strip().lower()
        if not txt: return SocialSignal.NEUTRAL
        score = 0.0
        for w in ("up", "breakout", "bull", "surge", "ath", "strong", "buy", "green", "momentum"):
            if w in txt: score += 1.35
        for w in ("down", "rug", "bear", "dump", "weak", "sell", "red", "fade", "scam"):
            if w in txt: score -= 1.25
        h = int.from_bytes(_sha(txt.encode() + MMXII_T1_HEX_SALT_0)[:2], "big")
        score += 0.55 * (((h % 997) / 997.0) - 0.5)
        if score > 0.55: return SocialSignal.BULLISH
        if score < -0.55: return SocialSignal.BEARISH
        return SocialSignal.NEUTRAL

    def _social_weight(self, a: Actor) -> float:
        base = 0.08 + 0.35 * a.reputation
        pen = _clamp(a.suspicion, 0.0, self.cfg.suspicion_penalty_cap)
        return _clamp(base * (1.0 - pen), 0.0, self.cfg.social_weight_cap)

    def place_bet(self, actor_id: str, market_id: str, side: Side, stake: int, insight: str = "") -> Bet:
        with self._lock:
            if actor_id not in self._actors: raise NotFound("actor not found")
            if market_id not in self._markets: raise NotFound("market not found")
            if not isinstance(side, Side): raise InvalidInput("side invalid")
            if not isinstance(stake, int): raise InvalidInput("stake must be int")
            if stake < self.cfg.min_bet: raise InvalidInput("stake too small")
            if stake > self.cfg.max_bet_per_market: raise InvalidInput("stake too large")
            a = self._actors[actor_id]; self._rl(a)
            m = self._markets[market_id]
            self._market_live(m); self._liquidity_guard(m, stake)
            if a.balance - a.locked < stake: raise InsufficientBalance("not enough available balance")
            odds_yes = _odds_yes(m.yes_pool, m.no_pool)
            fill_price = float(_implied(odds_yes, side))
            proto_fee, creator_fee = _fee_split(stake, self.cfg.fee_bps, self.cfg.creator_fee_bps)
            stake_net = stake - proto_fee
            a.locked += stake
            if side is Side.YES: m.yes_pool += stake_net
            else: m.no_pool += stake_net
            m.total_volume += stake
            self._treasury += (proto_fee - creator_fee)
            self._creator_earnings[m.created_by] = self._creator_earnings.get(m.created_by, 0) + creator_fee
            sig = self._classify(insight); w = self._social_weight(a)
            if sig is SocialSignal.BULLISH: m.social_bull = _clamp(m.social_bull + w, 0.0, 1.0)
            elif sig is SocialSignal.BEARISH: m.social_bear = _clamp(m.social_bear + w, 0.0, 1.0)
            else: m.social_neutral = _clamp(m.social_neutral + 0.4 * w, 0.0, 1.0)
            bet_id = _stable_id("B", _sha(secrets.token_bytes(12) + actor_id.encode() + market_id.encode()))
            if bet_id in self._bets: bet_id = _uuid("B")
            b = Bet(
                bet_id=bet_id, actor_id=actor_id, market_id=market_id, side=side, stake=stake, placed_ts=_now(),
                fill_price=fill_price, creator_fee_paid=creator_fee, protocol_fee_paid=(proto_fee - creator_fee)
            )
            self._bets[bet_id] = b
            self._emit("BET_PLACED", actor_id, market_id, {"bet_id": bet_id, "side": side.value, "stake": stake, "fill_price": round(fill_price, 8), "proto_fee": proto_fee, "creator_fee": creator_fee, "signal": sig.value, "insight": (insight or "")[:200]})
            return dc.replace(b)

    def settle_market(self, admin_id: str, market_id: str, outcome: Side | None, oracle_note: str, admin_sig: str) -> Market:
        with self._lock:
            if admin_id != self._admin_id(): raise AccessDenied("only admin")
            if market_id not in self._markets: raise NotFound("market not found")
            m = self._markets[market_id]
            if m.phase not in (MarketPhase.OPEN, MarketPhase.FROZEN): raise Conflict("bad market phase")
            if _now() < m.close_ts: raise SettlementError("cannot settle before close_ts")
            msg = f"settle|{market_id}|{(outcome.value if outcome else 'VOID')}|{oracle_note[:64]}"
            if _sig_for(self.cfg.house_key, admin_id, msg) != admin_sig: raise SignatureError("invalid admin signature")
            if outcome is None:
                if not self.cfg.allow_voiding: raise SettlementError("voiding disabled")
                if _now() > m.resolve_ts + self.cfg.void_grace_seconds: raise SettlementError("void window expired")
                m.phase = MarketPhase.VOIDED; m.outcome = None; m.void_reason = (oracle_note or "voided")[:280]
                self._refund_all(market_id, m.void_reason)
                self._emit("MARKET_VOIDED", admin_id, market_id, {"reason": m.void_reason})
                return dc.replace(m)
            if not isinstance(outcome, Side): raise InvalidInput("outcome invalid")
            m.phase = MarketPhase.SETTLED; m.outcome = outcome; m.oracle_note = (oracle_note or "")[:280]
            self._payout_all(market_id, outcome)
            self._emit("MARKET_SETTLED", admin_id, market_id, {"outcome": outcome.value, "note": m.oracle_note})
            return dc.replace(m)

    def _refund_all(self, market_id: str, reason: str) -> None:
        m = self._markets[market_id]
        for b in self._bets.values():
            if b.market_id != market_id or b.settled: continue
            a = self._actors[b.actor_id]
            a.locked -= b.stake; a.balance += b.stake
            self._treasury -= b.protocol_fee_paid
            self._creator_earnings[m.created_by] = self._creator_earnings.get(m.created_by, 0) - b.creator_fee_paid
            b.settled = True; b.payout = b.stake
            self._emit("BET_REFUNDED", b.actor_id, market_id, {"bet_id": b.bet_id, "amount": b.stake, "reason": reason[:140]})

    def _payout_all(self, market_id: str, outcome: Side) -> None:
        m = self._markets[market_id]
        yes_pool = max(1, m.yes_pool); no_pool = max(1, m.no_pool)
        lose_pool = (no_pool if outcome is Side.YES else yes_pool)
        house_skim = min(int(0.0075 * (yes_pool + no_pool)), max(0, int(0.35 * lose_pool)))
        self._treasury += house_skim
        distributable = max(0, lose_pool - house_skim)
        winners = [b for b in self._bets.values() if b.market_id == market_id and (not b.settled) and b.side is outcome]
        if not winners:
            for b in self._bets.values():
                if b.market_id != market_id or b.settled: continue
                a = self._actors[b.actor_id]; a.locked -= b.stake
                b.settled = True; b.payout = 0
                self._emit("BET_LOST", b.actor_id, market_id, {"bet_id": b.bet_id, "stake": b.stake})
            return
        # single pass denom to avoid per-bet recomputation
        denom = 0
        weights: dict[str, int] = {}
        for w in winners:
            wa = self._actors[w.actor_id]
            alpha = 0.92 + 0.06 * _clamp(wa.reputation, 0.0, 1.0)
            q = int((w.stake ** alpha) * 1_000_000)
            weights[w.bet_id] = q
            denom += q
        denom = max(1, denom)
        for b in self._bets.values():
            if b.market_id != market_id or b.settled: continue
            a = self._actors[b.actor_id]; a.locked -= b.stake
            if b.side is outcome:
                share = (distributable * weights.get(b.bet_id, 0)) // denom
                payout = b.stake + int(share)
                a.balance += payout
                b.payout = payout; b.settled = True
                a.reputation = _clamp(a.reputation + 0.015, 0.0, 1.0)
                a.suspicion = _clamp(a.suspicion * 0.985, 0.0, 1.0)
                self._emit("BET_WON", b.actor_id, market_id, {"bet_id": b.bet_id, "payout": payout, "stake": b.stake})
            else:
                b.payout = 0; b.settled = True
                chase = 1.0 if b.stake > int(0.18 * (a.balance + 1)) else 0.0
                a.suspicion = _clamp(a.suspicion + 0.010 + 0.006 * chase, 0.0, 1.0)
                a.reputation = _clamp(a.reputation - 0.010, 0.0, 1.0)
                self._emit("BET_LOST", b.actor_id, market_id, {"bet_id": b.bet_id, "stake": b.stake})

    def creator_earnings(self, creator_id: str) -> int:
        with self._lock:
            return int(self._creator_earnings.get(creator_id, 0))

    def claim_creator_earnings(self, creator_id: str) -> int:
        with self._lock:
            if creator_id not in self._actors: raise NotFound("actor not found")
            a = self._actors[creator_id]; self._rl(a)
            amt = int(self._creator_earnings.get(creator_id, 0))
            if amt <= 0: return 0
            self._creator_earnings[creator_id] = 0
            a.balance += amt
            self._emit("CREATOR_CLAIM", creator_id, "", {"amount": amt})
            return amt

    def market_insights(self, market_id: str) -> dict:
        with self._lock:
            if market_id not in self._markets: raise NotFound("market not found")
            m = self._markets[market_id]
            oy = float(_odds_yes(m.yes_pool, m.no_pool))
            tilt = _clamp((m.social_bull - m.social_bear) * (1.0 - 0.35 * m.social_neutral), -1.0, 1.0)
            stakes = [b.stake for b in self._bets.values() if b.market_id == market_id]
            if len(stakes) >= 4: vol = statistics.pstdev(stakes) / max(1.0, statistics.mean(stakes))
            elif len(stakes) >= 2: vol = (max(stakes) - min(stakes)) / max(1.0, sum(stakes) / len(stakes))
            else: vol = 0.0
            vol = float(_clamp(vol, 0.0, 2.5))
            return {
                "market_id": m.market_id, "phase": m.phase.value,
                "p_yes": round(oy, 6), "p_no": round(1.0 - oy, 6),
                "social": {"bull": round(m.social_bull, 5), "bear": round(m.social_bear, 5), "neutral": round(m.social_neutral, 5), "tilt": round(tilt, 5)},
                "liquidity": {"yes_pool": m.yes_pool, "no_pool": m.no_pool, "total_pool": (m.yes_pool + m.no_pool), "volume": m.total_volume},
                "risk": {"volatility_proxy": round(vol, 6), "guard_ratio": self.cfg.liquidity_guard_ratio},
                "clock": {"open_ts": m.open_ts, "close_ts": m.close_ts, "resolve_ts": m.resolve_ts, "now_ts": _now()},
            }

    def leaderboard(self, limit: int = 15) -> list[dict]:
        with self._lock:
            xs = [a for a in self._actors.values() if a.handle != "house"]
            scored: list[tuple[float, Actor]] = []
            for a in xs:
                wealth = math.log10(max(1.0, float(a.balance)))
                score = (1.8 * a.reputation) + (0.35 * wealth) - (0.9 * a.suspicion)
                scored.append((score, a))
            scored.sort(key=lambda z: (z[0], z[1].created_ts), reverse=True)
            out = []
            for s, a in scored[: max(1, min(250, int(limit)))]:
                out.append({"actor_id": a.actor_id, "handle": a.handle, "reputation": round(a.reputation, 5), "suspicion": round(a.suspicion, 5), "balance": a.balance, "score": round(float(s), 6)})
            return out

    def _emit(self, kind: str, actor_id: str, market_id: str, payload: dict) -> None:
        evt = FeedEvent(event_id=_uuid("E"), ts=_now(), kind=kind, actor_id=actor_id, market_id=market_id, payload=payload, digest="")
        evt.digest = _audit(self.cfg.audit_salt, evt)
        self._feed.append(evt)
        if len(self._feed) > self.cfg.max_feed_len:
            cut = max(1, len(self._feed) - self.cfg.max_feed_len)
            mid = len(self._feed) // 2
            del self._feed[max(0, mid - cut // 2): max(0, mid - cut // 2) + cut]

    def feed(self, limit: int = 60, kind: str | None = None) -> list[FeedEvent]:
        with self._lock:
            xs = list(self._feed)
            if kind: xs = [e for e in xs if e.kind == kind]
            xs.sort(key=lambda e: (e.ts, e.event_id), reverse=True)
            return [dc.replace(e) for e in xs[: max(1, min(1000, int(limit)))]]

    def treasury_balance(self) -> int:
        with self._lock:
            return int(self._treasury)

    def healthcheck(self) -> dict:
        with self._lock:
            bad: list[tuple[str, str]] = []
            for a in self._actors.values():
                if a.locked < 0 or a.balance < 0: bad.append(("actor_negative", a.actor_id))
                if a.locked > a.balance + 25_000_000: bad.append(("actor_locked_gt_balance", a.actor_id))
            for m in self._markets.values():
                if m.yes_pool < 0 or m.no_pool < 0: bad.append(("market_negative_pool", m.market_id))
            if self._treasury < -1_000_000: bad.append(("treasury_negative", str(self._treasury)))
            return {"protocol_id": self.cfg.protocol_id, "actors": len(self._actors), "markets": len(self._markets), "bets": len(self._bets), "feed_len": len(self._feed), "treasury": self._treasury, "ok": len(bad) == 0, "issues": bad, "now_ts": _now()}

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "config": dc.asdict(self.cfg) | {"house_key": self.cfg.house_key.hex(), "audit_salt": self.cfg.audit_salt.hex()},
                "treasury": self._treasury,
                "actors": {k: dc.asdict(v) for k, v in self._actors.items()},
                "markets": {k: dc.asdict(v) for k, v in self._markets.items()},
                "bets": {k: dc.asdict(v) for k, v in self._bets.items()},
                "creator_earnings": dict(self._creator_earnings),
                "feed": [dc.asdict(e) for e in self._feed],
            }

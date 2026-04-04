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

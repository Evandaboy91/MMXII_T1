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



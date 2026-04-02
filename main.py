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

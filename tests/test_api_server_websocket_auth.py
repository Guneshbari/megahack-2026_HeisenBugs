import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import api_server  # noqa: E402


class _RevokedTokenError(Exception):
    pass


class _ExpiredTokenError(Exception):
    pass


class _FirebaseAuthStub:
    RevokedIdTokenError = _RevokedTokenError
    ExpiredIdTokenError = _ExpiredTokenError

    def __init__(self, mode: str = "ok"):
        self.mode = mode

    def verify_id_token(self, token: str, check_revoked: bool = True):
        if self.mode == "revoked":
            raise self.RevokedIdTokenError("revoked")
        if self.mode == "expired":
            raise self.ExpiredIdTokenError("expired")
        if self.mode == "invalid":
            raise ValueError("invalid token")
        if self.mode == "missing_uid":
            return {"uid": ""}
        return {"uid": "user-123"}


def test_verify_websocket_token_missing_token(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", True, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("ok"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("")
    assert ok is False
    assert uid is None
    assert err == "missing_websocket_token"


def test_verify_websocket_token_sdk_not_ready(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", False, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("ok"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("token")
    assert ok is False
    assert uid is None
    assert err == "firebase_sdk_not_ready"


def test_verify_websocket_token_revoked(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", True, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("revoked"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("token")
    assert ok is False
    assert uid is None
    assert err == "revoked_token"


def test_verify_websocket_token_expired(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", True, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("expired"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("token")
    assert ok is False
    assert uid is None
    assert err == "expired_token"


def test_verify_websocket_token_missing_uid(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", True, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("missing_uid"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("token")
    assert ok is False
    assert uid is None
    assert err == "missing_uid"


def test_verify_websocket_token_success(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", True, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("ok"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("token")
    assert ok is True
    assert uid == "user-123"
    assert err == "ok"


def test_verify_websocket_token_invalid(monkeypatch):
    monkeypatch.setattr(api_server, "_FIREBASE_ADMIN_READY", True, raising=False)
    monkeypatch.setattr(api_server, "firebase_auth", _FirebaseAuthStub("invalid"), raising=False)
    ok, uid, err = api_server._verify_websocket_token("token")
    assert ok is False
    assert uid is None
    assert "invalid token" in err

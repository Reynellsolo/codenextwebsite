import hashlib
import secrets

try:
    from passlib.context import CryptContext
except Exception:  # pragma: no cover - optional dependency
    CryptContext = None

_pwd_context = None
if CryptContext is not None:
    _pwd_context = CryptContext(
        schemes=["argon2", "bcrypt"],
        deprecated="auto",
        argon2__memory_cost=65536,
        argon2__time_cost=3,
        argon2__parallelism=4,
    )


def hash_password(password: str) -> str:
    if _pwd_context is not None:
        return _pwd_context.hash(password)

    # Fallback hash format for environments without passlib/argon2.
    salt = secrets.token_hex(16)
    digest = hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()
    return f"sha256${salt}${digest}"


def verify_password(password: str, stored_hash: str) -> bool:
    if _pwd_context is not None and "$" in stored_hash and not stored_hash.startswith("sha256$"):
        try:
            return _pwd_context.verify(password, stored_hash)
        except Exception:
            return False

    try:
        algo, salt, digest = stored_hash.split("$", 2)
    except ValueError:
        hashlib.sha256(b"dummy").hexdigest()
        return False

    if algo != "sha256":
        return False

    current = hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()
    return secrets.compare_digest(current, digest)


def generate_token(size: int = 32) -> str:
    return secrets.token_urlsafe(size)

from pydantic import BaseModel, Field, validator
from typing import Optional, Dict


class _EmailMixin(BaseModel):
    email: str

    @validator("email")
    def validate_email(cls, v):
        value = (v or "").strip().lower()
        if "@" not in value or len(value) > 254:
            raise ValueError("Invalid email")
        return value


class RegisterRequest(_EmailMixin):
    password: str = Field(..., min_length=8, max_length=128)
    referral_code: Optional[str] = Field(default=None, max_length=20)


class LoginRequest(_EmailMixin):
    password: str = Field(..., min_length=8, max_length=128)


class ForgotPasswordRequest(_EmailMixin):
    pass


class ResetPasswordRequest(BaseModel):
    token: str = Field(..., min_length=20, max_length=512)
    new_password: str = Field(..., min_length=8, max_length=128)


class CreatePaymentDraftRequest(BaseModel):
    product: str = Field(..., max_length=50)
    method: str = Field(default="card", max_length=20)
    promo_code: Optional[str] = Field(default=None, max_length=40)


class WithdrawRequest(BaseModel):
    amount: int = Field(..., ge=1)
    note: Optional[str] = Field(default=None, max_length=500)

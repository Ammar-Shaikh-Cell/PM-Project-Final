from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, EmailStr


class EmailRecipientBase(BaseModel):
    email: EmailStr
    name: str | None = None
    is_active: bool = True
    description: str | None = None


class EmailRecipientCreate(EmailRecipientBase):
    pass


class EmailRecipientUpdate(BaseModel):
    email: EmailStr | None = None
    name: str | None = None
    is_active: bool | None = None
    description: str | None = None


class EmailRecipientResponse(EmailRecipientBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

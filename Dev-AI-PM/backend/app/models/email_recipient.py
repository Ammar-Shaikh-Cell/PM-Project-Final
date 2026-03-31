from sqlalchemy import Column, String, Boolean, Text
from pydantic import EmailStr

from app.models.base import Base


class EmailRecipient(Base):
    """Email notification recipients"""
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)  # Optional name/label for the recipient
    is_active = Column(Boolean, default=True, nullable=False)  # Enable/disable recipient
    description = Column(Text, nullable=True)  # Optional description/notes

from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.api.dependencies import get_session, get_current_user, require_engineer
from app.models.user import User
from app.models.email_recipient import EmailRecipient
from app.schemas.email_recipient import (
    EmailRecipientCreate,
    EmailRecipientUpdate,
    EmailRecipientResponse,
)

router = APIRouter(prefix="/email-recipients", tags=["email-recipients"])


@router.get("", response_model=List[EmailRecipientResponse])
async def list_email_recipients(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
):
    """List all email recipients"""
    result = await session.execute(
        select(EmailRecipient).order_by(EmailRecipient.created_at.desc())
    )
    recipients = result.scalars().all()
    return recipients


@router.post("", response_model=EmailRecipientResponse, status_code=status.HTTP_201_CREATED)
async def create_email_recipient(
    recipient: EmailRecipientCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
):
    """Create a new email recipient"""
    # Check if email already exists
    result = await session.execute(
        select(EmailRecipient).where(EmailRecipient.email == recipient.email)
    )
    existing = result.scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Email recipient with email {recipient.email} already exists",
        )

    new_recipient = EmailRecipient(
        email=recipient.email,
        name=recipient.name,
        is_active=recipient.is_active,
        description=recipient.description,
    )
    session.add(new_recipient)
    await session.commit()
    await session.refresh(new_recipient)
    return new_recipient


@router.get("/{recipient_id}", response_model=EmailRecipientResponse)
async def get_email_recipient(
    recipient_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
):
    """Get a specific email recipient"""
    result = await session.execute(
        select(EmailRecipient).where(EmailRecipient.id == recipient_id)
    )
    recipient = result.scalar_one_or_none()
    if not recipient:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Email recipient not found",
        )
    return recipient


@router.patch("/{recipient_id}", response_model=EmailRecipientResponse)
async def update_email_recipient(
    recipient_id: UUID,
    recipient_update: EmailRecipientUpdate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
):
    """Update an email recipient"""
    result = await session.execute(
        select(EmailRecipient).where(EmailRecipient.id == recipient_id)
    )
    recipient = result.scalar_one_or_none()
    if not recipient:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Email recipient not found",
        )

    # Check if email is being changed and if new email already exists
    if recipient_update.email and recipient_update.email != recipient.email:
        result = await session.execute(
            select(EmailRecipient).where(EmailRecipient.email == recipient_update.email)
        )
        existing = result.scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Email recipient with email {recipient_update.email} already exists",
            )

    # Update fields
    update_data = recipient_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(recipient, field, value)

    await session.commit()
    await session.refresh(recipient)
    return recipient


@router.delete("/{recipient_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_email_recipient(
    recipient_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_engineer),
):
    """Delete an email recipient"""
    result = await session.execute(
        select(EmailRecipient).where(EmailRecipient.id == recipient_id)
    )
    recipient = result.scalar_one_or_none()
    if not recipient:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Email recipient not found",
        )

    await session.delete(recipient)
    await session.commit()
    return None

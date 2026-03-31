"""Script to create demo users directly in the database"""
import asyncio
import sys
import os

# Ensure backend directory is in python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.session import AsyncSessionLocal
from app.core.security import get_password_hash
from app.models.user import User
from sqlalchemy import select

async def main():
    """Create demo users if they don't exist"""
    async with AsyncSessionLocal() as session:
        # Admin user
        result = await session.execute(select(User).where(User.email == "admin@example.com"))
        admin = result.scalars().first()
        if not admin:
            admin = User(
                email="admin@example.com",
                full_name="Admin User",
                role="admin",
                hashed_password=get_password_hash("admin123"),
            )
            session.add(admin)
            print("✓ Created admin user: admin@example.com / admin123")
        else:
            print("✓ Admin user already exists")
        
        # Engineer user
        result = await session.execute(select(User).where(User.email == "engineer@example.com"))
        engineer = result.scalars().first()
        if not engineer:
            engineer = User(
                email="engineer@example.com",
                full_name="Engineer User",
                role="engineer",
                hashed_password=get_password_hash("engineer123"),
            )
            session.add(engineer)
            print("✓ Created engineer user: engineer@example.com / engineer123")
        else:
            print("✓ Engineer user already exists")
        
        # Viewer user
        result = await session.execute(select(User).where(User.email == "viewer@example.com"))
        viewer = result.scalars().first()
        if not viewer:
            viewer = User(
                email="viewer@example.com",
                full_name="Viewer User",
                role="viewer",
                hashed_password=get_password_hash("viewer123"),
            )
            session.add(viewer)
            print("✓ Created viewer user: viewer@example.com / viewer123")
        else:
            print("✓ Viewer user already exists")
        
        await session.commit()
        print("\n✅ All demo users created/verified successfully!")

if __name__ == "__main__":
    asyncio.run(main())

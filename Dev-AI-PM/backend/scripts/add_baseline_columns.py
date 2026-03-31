"""Add baseline_learning and baseline_ready columns to profiles table"""
import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal

async def add_columns():
    async with AsyncSessionLocal() as session:
        try:
            # Add baseline_learning column
            await session.execute(text("""
                ALTER TABLE profiles 
                ADD COLUMN IF NOT EXISTS baseline_learning BOOLEAN NOT NULL DEFAULT false
            """))
            
            # Add baseline_ready column
            await session.execute(text("""
                ALTER TABLE profiles 
                ADD COLUMN IF NOT EXISTS baseline_ready BOOLEAN NOT NULL DEFAULT false
            """))
            
            await session.commit()
            print("✅ Successfully added baseline_learning and baseline_ready columns to profiles table")
        except Exception as e:
            print(f"❌ Error: {e}")
            await session.rollback()

if __name__ == "__main__":
    asyncio.run(add_columns())

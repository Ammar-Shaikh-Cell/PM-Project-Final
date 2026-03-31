"""Check and fix profiles table schema"""
import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal

async def check_and_fix():
    async with AsyncSessionLocal() as session:
        # Check current columns
        result = await session.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'profiles' 
            ORDER BY column_name
        """))
        columns = {row[0]: row[1] for row in result}
        print("Current profiles table columns:")
        for col, dtype in columns.items():
            print(f"  - {col}: {dtype}")
        
        # Check if baseline_learning exists
        if 'baseline_learning' not in columns:
            print("\n❌ baseline_learning column is missing!")
            print("Adding baseline_learning column...")
            await session.execute(text("""
                ALTER TABLE profiles 
                ADD COLUMN baseline_learning BOOLEAN NOT NULL DEFAULT false
            """))
            await session.commit()
            print("✅ Added baseline_learning column")
        else:
            print("✅ baseline_learning column exists")
        
        # Check if baseline_ready exists
        if 'baseline_ready' not in columns:
            print("\n❌ baseline_ready column is missing!")
            print("Adding baseline_ready column...")
            await session.execute(text("""
                ALTER TABLE profiles 
                ADD COLUMN baseline_ready BOOLEAN NOT NULL DEFAULT false
            """))
            await session.commit()
            print("✅ Added baseline_ready column")
        else:
            print("✅ baseline_ready column exists")

if __name__ == "__main__":
    asyncio.run(check_and_fix())

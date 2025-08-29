#!/usr/bin/env python3
"""
Database setup script for Fabric GPS email subscriptions
Run this script to create the email subscription tables
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.db_sqlserver import make_engine, Base, EmailSubscriptionModel, EmailVerificationModel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_subscription_tables():
    """Create email subscription tables in the database"""
    try:
        engine = make_engine()
        
        # Create tables
        logger.info("Creating email subscription tables...")
        Base.metadata.create_all(engine, tables=[
            EmailSubscriptionModel.__table__,
            EmailVerificationModel.__table__
        ])
        
        logger.info("Email subscription tables created successfully!")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

if __name__ == "__main__":
    setup_subscription_tables()

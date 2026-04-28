"""
Seeds the Cosh 2.0 database with:
- Admin user
- Language Registry (14 languages)
- Relationship Type Registry (14 foundational types)
- Product Registry (RootsTalk, PesTest)

Run once after alembic upgrade head: python scripts/seed_db.py
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from app.config import settings
from app.models.models import (
    User, UserRoleModel, LanguageRegistry, RelationshipTypeRegistry,
    ProductRegistry, UserRole, StatusEnum, TextDirection
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
engine = create_engine(settings.database_url_sync)


def seed():
    with Session(engine) as session:

        # ── Admin user ─────────────────────────────────────────────────────────
        existing_admin = session.query(User).filter_by(email=settings.admin_email).first()
        if existing_admin:
            print(f"Admin user already exists ({settings.admin_email}) — skipping.")
            admin = existing_admin
        else:
            print(f"Creating admin user: {settings.admin_email} ...")
            admin = User(
                email=settings.admin_email,
                name="Admin",
                password_hash=pwd_context.hash(settings.admin_password),
                status=StatusEnum.ACTIVE,
            )
            session.add(admin)
            session.flush()

            admin_role = UserRoleModel(
                user_id=admin.id,
                role=UserRole.ADMIN,
                status=StatusEnum.ACTIVE,
            )
            session.add(admin_role)
            session.flush()
            print(f"  ✓ Admin created with id: {admin.id}")

        # ── Language Registry ──────────────────────────────────────────────────
        languages = [
            ("en", "English",    "English",     "Latin",      "LTR"),
            ("hi", "Hindi",      "हिन्दी",        "Devanagari", "LTR"),
            ("ta", "Tamil",      "தமிழ்",         "Tamil",      "LTR"),
            ("te", "Telugu",     "తెలుగు",         "Telugu",     "LTR"),
            ("kn", "Kannada",    "ಕನ್ನಡ",          "Kannada",    "LTR"),
            ("ml", "Malayalam",  "മലയാളം",        "Malayalam",  "LTR"),
            ("mr", "Marathi",    "मराठी",          "Devanagari", "LTR"),
            ("gu", "Gujarati",   "ગુજરાતી",        "Gujarati",   "LTR"),
            ("pa", "Punjabi",    "ਪੰਜਾਬੀ",          "Gurmukhi",   "LTR"),
            ("or", "Odia",       "ଓଡ଼ିଆ",           "Odia",       "LTR"),
            ("bn", "Bengali",    "বাংলা",          "Bengali",    "LTR"),
            ("ur", "Urdu",       "اردو",           "Nastaliq",   "RTL"),
            ("as", "Assamese",   "অসমীয়া",        "Bengali",    "LTR"),
        ]

        for code, name_en, name_native, script, direction in languages:
            existing = session.query(LanguageRegistry).filter_by(language_code=code).first()
            if existing:
                print(f"  Language {code} already exists — skipping.")
                continue
            lang = LanguageRegistry(
                language_code=code,
                language_name_en=name_en,
                language_name_native=name_native,
                script=script,
                direction=TextDirection.RTL if direction == "RTL" else TextDirection.LTR,
                status=StatusEnum.ACTIVE,
                added_by=admin.id,
            )
            session.add(lang)
            print(f"  ✓ Language: {name_en} ({code})")

        # ── Relationship Type Registry ─────────────────────────────────────────
        rel_types = [
            ("IS_A",               "Is A",               "Classification — source belongs to a category.",           "Paddy IS_A Crop"),
            ("AFFECTS",            "Affects",             "Source has an agronomic impact on the target.",            "Aphid AFFECTS Tomato"),
            ("PRODUCES",           "Produces",            "Source causes an observable symptom or outcome.",          "Stem Borer PRODUCES Deadheart"),
            ("IS_ACTIVE_DURING",   "Is Active During",    "Source is present or relevant during target time window.", "Root Knot Nematode IS_ACTIVE_DURING Seedling Stage"),
            ("IS_MANUFACTURED_BY", "Is Manufactured By",  "Source product is produced by the target manufacturer.",   "Confidor IS_MANUFACTURED_BY Bayer CropScience"),
            ("HAS_PARAMETER",      "Has Parameter",       "Source crop has the target as a guided elimination parameter.", "Paddy HAS_PARAMETER Season"),
            ("HAS_VARIABLE",       "Has Variable",        "Source parameter has the target as an answer option.",     "Season HAS_VARIABLE Kharif"),
            ("IS_INDICATOR_FOR",   "Is Indicator For",    "Source observable is a harvest readiness signal for target.", "Fruit Colour Change IS_INDICATOR_FOR Mango"),
            ("HAS_DUS_CHARACTER",  "Has DUS Character",   "Source crop has the target as a registered seed variety descriptor.", "Paddy HAS_DUS_CHARACTER Grain Length"),
            ("HAS_IMAGE",          "Has Image",           "Source knowledge item is illustrated by the target image.", "Early Blight on Tomato Leaf HAS_IMAGE [image]"),
            ("REQUIRES",           "Requires",            "Source depends on the target.",                            "Paddy REQUIRES Nitrogen"),
            ("IS_CAUSED_BY",       "Is Caused By",        "Source results from the target.",                          "Wilt IS_CAUSED_BY Root Rot"),
            ("IS_APPLIED_DURING",  "Is Applied During",   "Source input is applied at the target time.",              "Basal Fertiliser IS_APPLIED_DURING Land Preparation"),
            ("IS_USED_FOR",        "Is Used For",         "Source is used in the context of the target.",             "Trichogramma IS_USED_FOR Stem Borer Management"),
        ]

        for label, display_name, description, example in rel_types:
            existing = session.query(RelationshipTypeRegistry).filter_by(label=label).first()
            if existing:
                print(f"  Relationship type {label} already exists — skipping.")
                continue
            rt = RelationshipTypeRegistry(
                label=label,
                display_name=display_name,
                description=description,
                example=example,
                added_by=admin.id,
            )
            session.add(rt)
            print(f"  ✓ Relationship type: {label}")

        # ── Product Registry ───────────────────────────────────────────────────
        products = [
            ("rootstalk", "RootsTalk"),
            ("pestest",   "PesTest"),
        ]

        for name, display_name in products:
            existing = session.query(ProductRegistry).filter_by(name=name).first()
            if existing:
                print(f"  Product {name} already exists — skipping.")
                continue
            product = ProductRegistry(
                name=name,
                display_name=display_name,
                status=StatusEnum.ACTIVE,
                added_by=admin.id,
            )
            session.add(product)
            print(f"  ✓ Product: {display_name}")

        session.commit()
        print("\nDatabase seeded successfully.")


if __name__ == "__main__":
    seed()

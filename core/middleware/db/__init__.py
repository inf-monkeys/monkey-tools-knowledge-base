from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def init_app(app):
    db.init_app(app)

def handle_invalid_transaction():
    if db.session.is_active:
        db.session.rollback()

def commit_with_rollback():
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()

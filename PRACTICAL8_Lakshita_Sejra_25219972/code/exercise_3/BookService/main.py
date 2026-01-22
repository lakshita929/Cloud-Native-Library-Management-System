from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
import os
import time

app = Flask(__name__)

# -------------------------------
# DATABASE CONFIG
# -------------------------------
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")

app.config["SQLALCHEMY_DATABASE_URI"] = (
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# -------------------------------
# DB MODEL
# -------------------------------
class Book(db.Model):
    __tablename__ = "books"
    bookid = db.Column(db.String(20), primary_key=True)
    title = db.Column(db.String(50), nullable=False)
    author = db.Column(db.String(50), nullable=False)

    def to_dict(self):
        return dict(bookid=self.bookid, title=self.title, author=self.author)


# -------------------------------
# DB READY CHECK
# -------------------------------
def wait_for_db():
    while True:
        try:
            with app.app_context():
                db.session.execute(text("SELECT 1"))
            print("BookService: Database is ready")
            break
        except OperationalError as e:
            print(f"BookService: Database not ready yet, retrying in 3 seconds: {e}")
            time.sleep(3)


wait_for_db()
with app.app_context():
    db.create_all()


# -------------------------------
# ROUTES
# -------------------------------
@app.route("/books/add", methods=["POST"])
def create_book():
    data = request.get_json(force=True)
    book = Book(**data)
    db.session.add(book)
    db.session.commit()
    return jsonify(book.to_dict()), 201


@app.route("/books/all", methods=["GET"])
def get_books():
    books = Book.query.all()
    return jsonify([u.to_dict() for u in books]), 200


@app.route("/books/<bookid>", methods=["GET"])
def get_book(bookid):
    book = Book.query.get(bookid)
    if not book:
        return jsonify({"error": "Book not found"}), 404
    return jsonify(book.to_dict()), 200


@app.route("/books/<bookid>", methods=["PUT"])
def update_book(bookid):
    book = Book.query.get(bookid)
    if not book:
        return jsonify({"error": "Book not found"}), 404

    data = request.get_json(force=True)
    if "title" in data:
        book.title = data["title"]
    if "author" in data:
        book.author = data["author"]

    db.session.commit()
    return jsonify(book.to_dict()), 200


@app.route("/books/<bookid>", methods=["DELETE"])
def delete_book(bookid):
    book = Book.query.get(bookid)
    if not book:
        return jsonify({"error": "Book not found"}), 404

    db.session.delete(book)
    db.session.commit()
    return jsonify({"message": "Book deleted successfully"}), 200


# -------------------------------
# STARTUP
# -------------------------------
if __name__ == "__main__":
    print("BookService running on port 5006")
    app.run(host="0.0.0.0", port=5006)
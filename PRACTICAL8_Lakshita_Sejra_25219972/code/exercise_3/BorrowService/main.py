# BorrowService/main.py

import json
import os
import time
import threading

import pika
import requests
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

app = Flask(__name__)

# -------------------------------
# DATABASE CONFIG
# -------------------------------
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")

app.config["SQLALCHEMY_DATABASE_URI"] = (
    f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# -------------------------------
# DB MODEL
# -------------------------------
class Borrow(db.Model):
    __tablename__ = "borrows"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    student_id = db.Column(db.String(20), nullable=False)
    book_id = db.Column(db.String(20), nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "student_id": self.student_id,
            "book_id": self.book_id,
        }


# -------------------------------
# DB READY CHECK
# -------------------------------
def wait_for_db():
    while True:
        try:
            with app.app_context():
                db.session.execute(text("SELECT 1"))
            print("BorrowService: Database is ready")
            break
        except OperationalError as e:
            print("BorrowService: Database not ready yet, retrying in 3 seconds:", e)
            time.sleep(3)


wait_for_db()
with app.app_context():
    db.create_all()


# -------------------------------
# RABBITMQ CONFIG
# -------------------------------
RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_DEFAULT_PASS")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))

print("BorrowService env RABBITMQ_USER:", RABBITMQ_USER)
print("BorrowService env RABBITMQ_PASS:", RABBITMQ_PASS)
print("BorrowService env RABBITMQ_HOST:", RABBITMQ_HOST)
print("BorrowService env RABBITMQ_PORT:", RABBITMQ_PORT)


# -------------------------------
# VALIDATION HELPERS
# -------------------------------
def student_exists(student_id):
    # Use Kubernetes service name: user-service
    url = f"http://user-service:5002/users/{student_id}"
    try:
        r = requests.get(url, timeout=3)
        return r.status_code == 200
    except requests.RequestException as e:
        print("Error calling UserService:", e)
        return False


def book_exists(book_id):
    # Use Kubernetes service name: book-service
    url = f"http://book-service:5006/books/{book_id}"
    try:
        r = requests.get(url, timeout=3)
        return r.status_code == 200
    except requests.RequestException as e:
        print("Error calling BookService:", e)
        return False


# -------------------------------
# RABBITMQ CONSUMER
# -------------------------------
def process_borrow_request(ch, method, properties, body):
    data = json.loads(body)
    student_id = data.get("student_id")
    book_id = data.get("book_id")

    print("Received borrow request:", data)

    # Use Flask app context inside RabbitMQ thread
    with app.app_context():
        if not student_exists(student_id):
            print("Invalid student ID, rejecting")
            return

        if not book_exists(book_id):
            print("Invalid book ID, rejecting")
            return

        count = Borrow.query.filter_by(student_id=student_id).count()
        if count >= 5:
            print("Student already has 5 books, rejecting")
            return

        new_borrow = Borrow(student_id=student_id, book_id=book_id)
        db.session.add(new_borrow)
        db.session.commit()

        print("Borrow request saved successfully")


def start_rabbitmq_listener():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )

    while True:
        try:
            print(
                f"BorrowService: Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}"
            )
            connection = pika.BlockingConnection(params)
            break
        except pika.exceptions.AMQPConnectionError as e:
            print("BorrowService: RabbitMQ connection failed, retrying in 5 seconds:", e)
            time.sleep(5)

    channel = connection.channel()
    channel.queue_declare(queue="borrow_book", durable=True)

    print("BorrowService listening on RabbitMQ queue: borrow_book")

    channel.basic_consume(
        queue="borrow_book",
        on_message_callback=process_borrow_request,
        auto_ack=True,
    )

    channel.start_consuming()


# -------------------------------
# API ROUTES
# -------------------------------
@app.route("/borrow/<student_id>", methods=["GET"])
def get_borrowed(student_id):
    borrowed = Borrow.query.filter_by(student_id=student_id).all()
    return jsonify([b.to_dict() for b in borrowed]), 200


# -------------------------------
# STARTUP
# -------------------------------
if __name__ == "__main__":
    listener_thread = threading.Thread(target=start_rabbitmq_listener)
    listener_thread.daemon = True
    listener_thread.start()

    print("BorrowService running on port 5004")
    app.run(host="0.0.0.0", port=5004)

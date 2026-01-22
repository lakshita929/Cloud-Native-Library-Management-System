from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
import os
import pika
import json
import time

# =======================
# POSTGRES CONFIG
# =======================

db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = (
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# =======================
# DATABASE MODELS
# =======================
class User(db.Model):
    __tablename__ = "users"
    studentid = db.Column(db.String(20), primary_key=True)
    firstname = db.Column(db.String(50), nullable=False)
    lastname = db.Column(db.String(50), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    def to_dict(self):
        return dict(
            studentid=self.studentid,
            firstname=self.firstname,
            lastname=self.lastname,
            email=self.email,
        )


# =======================
# DB READY CHECK
# =======================
def wait_for_db():
    while True:
        try:
            with app.app_context():
                db.session.execute(text("SELECT 1"))
            print("UserService: Database is ready")
            break
        except OperationalError as e:
            print(f"UserService: Database not ready yet, retrying in 3 seconds: {e}")
            time.sleep(3)


wait_for_db()
with app.app_context():
    db.create_all()


# =======================
# RABBITMQ CONFIG
# =======================
rabbit_user = os.getenv("RABBITMQ_DEFAULT_USER")
rabbit_pass = os.getenv("RABBITMQ_DEFAULT_PASS")
rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
rabbit_port = int(os.getenv("RABBITMQ_PORT", 5672))


def get_rabbitmq_channel():
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    params = pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        virtual_host="/",
        credentials=credentials,
    )

    while True:
        try:
            print(f"UserService: Connecting to RabbitMQ at {rabbit_host}:{rabbit_port}")
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            return channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"UserService: RabbitMQ unavailable, retrying in 5 seconds: {e}")
            time.sleep(5)


# =======================
# CRUD ENDPOINTS
# =======================
@app.route("/users/add", methods=["POST"])
def create_user():
    data = request.get_json(force=True)
    user = User(**data)
    db.session.add(user)
    db.session.commit()
    return jsonify(user.to_dict()), 201


@app.route("/users/all", methods=["GET"])
def get_users():
    users = User.query.all()
    return jsonify([u.to_dict() for u in users]), 200


@app.route("/users/<studentid>", methods=["GET"])
def get_user(studentid):
    user = User.query.get(studentid)
    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify(user.to_dict()), 200


@app.route("/users/<studentid>", methods=["PUT"])
def update_user(studentid):
    user = User.query.get(studentid)
    if not user:
        return jsonify({"error": "User not found"}), 404

    data = request.get_json(force=True)
    if "firstname" in data:
        user.firstname = data["firstname"]
    if "lastname" in data:
        user.lastname = data["lastname"]

    if "email" in data:
        if User.query.filter(
            User.email == data["email"], User.studentid != studentid
        ).first():
            return jsonify({"error": "Email already exists"}), 400
        user.email = data["email"]

    db.session.commit()
    return jsonify(user.to_dict()), 200


@app.route("/users/<studentid>", methods=["DELETE"])
def delete_user(studentid):
    user = User.query.get(studentid)
    if not user:
        return jsonify({"error": "User not found"}), 404
    db.session.delete(user)
    db.session.commit()
    return jsonify({"message": "User deleted successfully"}), 200


# =======================
# BORROW REQUEST ENDPOINT
# =======================
@app.route("/users/borrow/request", methods=["POST"])
def borrow_book():
    data = request.get_json()

    required_fields = {"student_id", "book_id", "date_returned"}
    if not data or not required_fields.issubset(data):
        return jsonify({"error": "Invalid data format"}), 400

    channel = get_rabbitmq_channel()
    channel.basic_publish(
        exchange="",
        routing_key="borrow_book",
        body=json.dumps(data),
    )

    return jsonify(
        {
            "message": "Borrow requested",
            "request": data,
        }
    ), 201


# =======================
# START FLASK
# =======================
if __name__ == "__main__":
    print("UserService running on port 5002")
    app.run(host="0.0.0.0", port=5002)
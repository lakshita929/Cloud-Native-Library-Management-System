from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import os

db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('POSTGRES_HOST')
db_port = os.getenv('POSTGRES_PORT')
db_name = os.getenv('POSTGRES_DB')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class User(db.Model):
    __tablename__ = 'users'
    studentid = db.Column(db.String(20), primary_key=True)
    firstname = db.Column(db.String(50), nullable=False)
    lastname  = db.Column(db.String(50), nullable=False)
    email     = db.Column(db.String(120), unique=True, nullable=False)

    def to_dict(self):
        return dict(studentid=self.studentid, firstname=self.firstname, lastname=self.lastname, email=self.email)

with app.app_context():
    db.create_all()

@app.route('/users/add', methods=['POST'])
def create_user():
    data = request.get_json(force=True)
    user = User(**data)
    db.session.add(user)
    db.session.commit()
    return jsonify(user.to_dict()), 201

@app.route('/users/all', methods=['GET'])
def get_users():
    users = User.query.all()
    return jsonify([u.to_dict() for u in users]), 200

@app.route('/users/<studentid>', methods=['GET'])
def get_user(studentid):
    user = User.query.get(studentid)
    if not user:
        return jsonify({"error": "User not found"}), 404
    return jsonify(user.to_dict()), 200

@app.route('/users/<studentid>', methods=['PUT'])
def update_user(studentid):
    user = User.query.get(studentid)
    if not user:
        return jsonify({"error": "User not found"}), 404
    data = request.get_json(force=True)
    if 'firstname' in data: user.firstname = data['firstname']
    if 'lastname' in data: user.lastname = data['lastname']
    if 'email' in data:
        # prevent duplicate email on another user
        if User.query.filter(User.email == data['email'], User.studentid != studentid).first():
            return jsonify({"error": "Email already exists"}), 400
        user.email = data['email']
    db.session.commit()
    return jsonify(user.to_dict()), 200

@app.route('/users/<studentid>', methods=['DELETE'])
def delete_user(studentid):
    user = User.query.get(studentid)
    if not user:
        return jsonify({"error": "User not found"}), 404
    db.session.delete(user)
    db.session.commit()
    return jsonify({"message": "User deleted successfully"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)

from flask import Flask, jsonify, request
from models import Department, Employee
from .db import db

def create_app():
    app = Flask(__name__)
    db.model_init(Department)
    db.model_init(Employee)
    db.upgrade()
    

    @app.route('/', methods=['GET', 'POST'])
    def find_user():
        data = db.getSchema()
        return jsonify(data)
    
    return app

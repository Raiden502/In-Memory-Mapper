from flask import Flask, jsonify, request
from models import UserInfo, LibraryInfo
from replq.sessions import Session
from .db import database

def create_app():
    app = Flask(__name__)
    database.addTable(UserInfo)
    database.addTable(LibraryInfo)
    
    @app.route('/add', methods=['POST'])
    def create_user():
        session:Session =  Session()
        user_data = request.json
        
        new_user:UserInfo = UserInfo(**user_data)
        session.add(new_user)
        session.commit()
    
        return jsonify({
            "status":"Inserted"
        })

    @app.route('/all', methods=['GET', 'POST'])
    def find_user():
        data = database.getDbProperties()
        return jsonify(data)
    
    return app

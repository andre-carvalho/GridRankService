#!/usr/bin/python3
from flask import Flask, make_response, jsonify, abort
from rankcell_dao import RankCellDao

api = Flask(__name__)

@api.route('/rankcell/api/task', methods=['GET'])
def makerank():
    try:
        db = RankCellDao()
        db.createTable()
    except Exception as error:
        not_found(error)
    

@api.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

if __name__ == "__main__":
    api.run(host='0.0.0.0')
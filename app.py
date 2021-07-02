from flask import Flask, jsonify, request
from operationBD import koha



app = Flask(__name__)

@app.route ('/get', methods = ['POST'])
def get():            
    kohas =koha()
    res =kohas.getJson(request.json)
    return jsonify(res)


@app.route ('/insert', methods = ['POST'])
def insert():            
    kohas =koha()
    res = kohas.insertJson(request.json )
    return jsonify(res)


@app.route ('/update', methods = ['POST'])
def update():            
    kohas =koha()
    res =kohas.updateJson(request.json)
    return jsonify(res)
     








if __name__ == '__main__':
    app.run(debug=True, port=4000)
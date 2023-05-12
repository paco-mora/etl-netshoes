from flask import Flask
from flask_cors import CORS
from blueprints.import_file import upload_blueprint
from blueprints.netshoes import etl_netshoes_blueprint
from blueprints.tasks import tasks_blueprint
from blueprints.export_netshoes import export_netshoes_blueprint

app = Flask(__name__)
CORS(app)

app.register_blueprint(upload_blueprint, url_prefix='/upload')
app.register_blueprint(etl_netshoes_blueprint, url_prefix='/etl-netshoes')
app.register_blueprint(tasks_blueprint, url_prefix='/tasks')
app.register_blueprint(export_netshoes_blueprint, url_prefix='/export')

@app.route("/")
def hello():
    return "<h1 style='color:blue'>Hello Cu!!</h1>"

if __name__ == '__main__':
    app.run(debug=True)
    app.run(debug=True,ssl_context='adhoc')

from flask import Flask, request, render_template, send_from_directory
from flask_cors import CORS
from module_common_api  import module_common_api
app = Flask(__name__, static_folder='static', template_folder='static')


app.register_blueprint(module_common_api)
CORS(app)
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls', '.json', '.xml']
app.config['MAX_CONTENT_LENGTH'] = 10000* 1024 * 1024

@app.route("/")
def homepage():
  return render_template('index.html')

@app.route("/static/")
def homepage_new():
  return render_template('index.html')




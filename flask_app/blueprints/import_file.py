import os
import urllib.request
from flask import Flask, flash, request, redirect, render_template, Blueprint
from werkzeug.utils import secure_filename
from conf import NETSHOES_FOLDER

upload_blueprint = Blueprint('upload', __name__)

ALLOWED_EXTENSIONS = set(['xlsx', 'csv', 'xls', 'jpg', 'jpeg', 'gif'])


def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def file_extension(filename):
	return filename.rsplit('.', 1)[1].lower()

	
@upload_blueprint.route('/')
def upload_form():
	return render_template('upload.html')

@upload_blueprint.route('/etl-netshoes/file-name/<file_name>/user/<user_id>', methods=['POST'])
def upload_file(file_name, user_id):
	if(request.method == 'POST'):
		if('file' not in request.files):
			return 'Sem arquivo!'
		file = request.files['file']
		if(file.filename == ''):
			return 'Vazio'
		if(file and allowed_file(file.filename)):
			file.save(os.path.join(NETSHOES_FOLDER, str(file_name)+'_'+str(user_id)+'.'+str(file_extension(file.filename))))
			return '200'
		else:
			return 'Allowed file types are txt, pdf, png, jpg, jpeg, gif'
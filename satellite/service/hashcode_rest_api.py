import json
from flask import Flask, request
from hashcode_service import check_files


app = Flask(__name__)
ALLOWED_EXTENSIONS = {'xls', 'yaml', 'csv', 'json'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


@app.route('/hashcode', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        files_dict = {}
        namespace = request.values.get('namespace')
        if not namespace:
            return "[ERROR] Please input namespace."
        files = request.files.getlist('file')
        if files:
            for file in files:
                if file and allowed_file(file.filename):
                    content = file.read()
                    files_dict[file.filename] = content
                    print(file.filename)
        keys = request.form.getlist('key')
        if keys:
            for key in keys:
                files_dict[key] = key
        print(files_dict)
        outputs = check_files(files_dict, namespace=namespace)
        return json.dumps(outputs, indent=4)
    return "Hello, this is a hashcode service. Here are some curl commands. " \
           "<br> For files: <br> <li> curl -F 'namespace=&lt;Your satellite namespace&gt;' -F 'file=@&lt;somefiles&gt;' http://localhost:5000/hashcode" \
           "<br> For classes: <br> <li> curl -d 'namespace=&lt;Your satellite namespace&gt;' -d 'key=&lt;class name&gt;' http://localhost:5000/hashcode"
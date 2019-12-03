from flask import Flask, request
from reservation_service import get_qnode


app = Flask(__name__)
ALLOWED_EXTENSIONS = {'xls', 'yaml', 'csv', 'json'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


@app.route('/reservation', methods=['GET', 'POST'])
def get_qnode_by_ns():
    if request.method == 'POST':
        namespace = request.values.get('namespace')
        output = get_qnode(namespace)
        return output
    return "Hello, this is a wikidata reservation service. Here is a curl command about how to use it. " \
           "<br> <li> curl -d 'namespace=&lt;Your satellite namespace&gt;' http://localhost:5000/reservation"

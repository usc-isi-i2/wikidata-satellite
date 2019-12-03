import os
import json
import hashlib
from reservation_service import get_qnode


dir = os.path.split(os.path.realpath(__file__))[0]
path = os.path.join(dir, 'hashcode.json')


def read_data():
    if os.path.exists(path):
        with open(path, 'r') as f:
            content = f.read()
            data = json.loads(content)
    else:
        data = {}
    return data


def write_data(data):
    content = json.dumps(data, indent=4)
    with open(path, 'w') as f:
        f.write(content)


def check_files(files_dict, namespace) -> dict():
    data = read_data()
    outputs = {}
    for name, content in files_dict.items():
        if type(content) is bytes:
            hash_code = hashlib.md5(content).hexdigest()
        else:
            hash_code = hashlib.md5(content.encode()).hexdigest()
        if namespace not in data.keys():
            data[namespace] = {}
        if namespace in data.keys() and hash_code not in data[namespace].keys():
            data[namespace][hash_code] = get_qnode(namespace)
        outputs[name] = {'hash_code': hash_code, 'qnode': data[namespace][hash_code]}
    write_data(data)
    return outputs


# if __name__ == "__main__":
#     files_dict = {
#         'dataset1_name': 'apple_content',
#         'wikifier_name': 'banana_content',
#         'mapping_file_name': 'cat_content',
#         'dataset2_name': 'dog_content',
#         'dataset3_name': 'apple_content'
#     }
#     outputs = check_files(files_dict, 'sc')
#     print(outputs)

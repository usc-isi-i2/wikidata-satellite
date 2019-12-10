import yaml
import pandas as pd
import json
import requests
import sys

from etk.knowledge_graph import KGSchema
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.wikidata.entity import WDItem, WDProperty
from etk.wikidata.value import Item, Property, MonolingualText, URLValue, StringValue, Datatype, QuantityValue
from etk.wikidata.__init__ import create_custom_prefix
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from lib.hashcode import Hashcode


class SchemaOfSatelliteDataset:
    def __init__(self, config_path):
        self.data = self.read_data(config_path)

        self.ns = self.data['satelliteNamespace']['namespace']
        self.uri = self.data['satelliteNamespace']['uri']

    @staticmethod
    def read_data(file_path):
        format = file_path.rsplit('.', 1)[1]
        with open(file_path, 'r') as f:
            if format == 'json':
                data = json.loads(f.read())
            elif format == 'yaml':
                data = yaml.safe_load(f)
            else:
                data = f.read()
        return data

    def write_data(self, doc, filename, format='ttl'):
        file_path = self.data['output'] + filename
        f = open(file_path, 'w')
        f.write(doc.kg.serialize(format))
        print('Serialization completed!')

    def register_ns_in_reservation_service(self):
        # check ns if exists
        response = requests.post(self.data['reservationEndpoint'] + '/' + self.ns)
        if response.status_code != 200:
            # if not existed, register first
            requests.post(self.data['reservationEndpoint'] + '/register',
                          data={'namespace': self.ns, 'uri': self.uri,
                                'prefix': self.data['qnodePrefix'], 'num_of_0': self.data['numOf0']})

    def call_reservation_service(self, path):
        # check file if exists
        hc = Hashcode(self.data['satelliteEndpoint'])
        qnode, hashcode = hc.check_files(self.data['satelliteNamespace']['uri'], path)

        # qnode exists
        if qnode:
            return qnode, hashcode, True

        # call reservation service
        reservation = requests.post(self.data['reservationEndpoint'] + '/' + self.ns + '/reservation')
        if reservation.status_code == 200:
            qnode = reservation.content.decode("utf-8")
            qnode = json.loads(qnode)['Latest qnode']

        return qnode, hashcode, False

    def extract_files(self):
        self.register_ns_in_reservation_service()
        for k, v in self.data['inputs'].items():
            if k != 'metadata':
                if k == 'dataset':
                    md_path = self.data['inputs']['metadata']['path']
                    ds_path = self.data['inputs']['dataset']['path']
                    # call service to get qnode
                    qnode, hashcode, existed = self.call_reservation_service(md_path)
                    # extract metadata and dataset
                    md = self.read_data(md_path)
                    self.data['inputs']['dataset']['content'] = {
                        'keywords': md['keywords'],
                        'description': md['description'],
                        'title': md['title'],
                        'variable_measured': [],
                        'filename': ds_path.split('/')[-1].split('.')[0]
                    }
                    if self.data['storeColumnValue']:
                        self.extract_dataset(ds_path)
                else:
                    # call service to get qnode
                    qnode, hashcode, existed = self.call_reservation_service(v['path'])
                    # extract wikifier and mapping file
                    self.data['inputs'][k]['content'] = self.read_data(v['path'])
                self.data['inputs'][k]['qnode'] = qnode
                self.data['inputs'][k]['hashcode'] = hashcode
                self.data['inputs'][k]['existed'] = existed

    def extract_dataset(self, ds_path):
        if ds_path.endswith('.xls'):
            df = pd.read_excel(ds_path, skiprows=[0, 1, 2, 3, 4]).iloc[0:-3, 1:13]
        elif ds_path.endswith('.csv'):
            df = pd.read_csv(ds_path)

        for i in range(df.shape[1]):
            if df.iloc[:, i].dtypes == 'float64':
                semantic_type_url = 'http://schema.org/Float'
                data_type = 'float'
            elif df.iloc[:, i].dtypes == 'int64':
                data_type = 'int'
                semantic_type_url = 'http://schema.org/Integer'
            else:
                data_type = 'string'
                semantic_type_url = 'http://schema.org/Text'

            values = ' '.join(str(i) for i in df.iloc[:, i].tolist())

            self.data['inputs']['dataset']['content']['variable_measured'].append({
                'column_name': df.columns[i],
                'values_of_a_column': values,
                'data_structure_type': data_type,
                'semantic_type_identifier': semantic_type_url,
                'column_index': i
            })

    def model_statement(self):
        # initialize KGSchema
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        doc = etk.create_document({}, doc_id='http://isi.edu/default-ns/projects')

        # bind prefix
        doc = create_custom_prefix(doc, custom_dict={self.ns: self.uri})

        # extract files
        self.extract_files()

        # model statement
        inputs = self.data['inputs']
        for k, v in inputs.items():
            if k != 'metadata':
                # construct wikifier instance
                if k == 'wikifier' and not v['existed']:
                    q = WDItem(v['qnode'], namespace=self.ns, creator=':datamart')
                    q.add_label('A wikifier file for ' + inputs['dataset']['content']['filename'], lang='en')
                    q.add_statement('P31', Item('SDQ1001', namespace=self.ns))  # an instance of Wikifier
                    q.add_statement('P127', Item('SDQ1003', namespace=self.ns))     # belongs to
                    q.add_statement('SDP3003', StringValue(v['content']), namespace=self.ns)           # hasFileContent
                    q.add_statement('SDP3004', StringValue(v['hashcode']), namespace=self.ns)               # hashValue

                # construct mapping_file instance
                elif k == 'mappingFile' and not v['existed']:
                    q = WDItem(v['qnode'], namespace=self.ns, creator=':datamart')
                    q.add_label('A mapping file for ' + inputs['dataset']['content']['filename'], lang='en')
                    q.add_statement('P31', Item('SDQ1002', namespace=self.ns))  # an instance of MappingFile
                    q.add_statement('P170', StringValue('T2WML'))
                    q.add_statement('P127', Item('SDQ1003', namespace=self.ns))
                    q.add_statement('SDP3003', StringValue(json.dumps(v['content'])), namespace=self.ns)
                    q.add_statement('SDP3004', StringValue(v['hashcode']), namespace=self.ns)

                # construct dataset instance
                elif k == 'dataset' and not v['existed']:
                    q = WDItem(v['qnode'], namespace=self.ns, creator=':datamart')
                    q.add_label(v['content']['title'], lang='en')
                    q.add_description(v['content']['description'], lang='en')
                    q.add_statement('P31', Item('Q1172284'))  # an instance of Dataset
                    q.add_statement('SDP3001', Item(inputs['wikifier']['qnode'], namespace=self.ns), namespace=self.ns) # a wikifier file
                    q.add_statement('SDP3002', Item(inputs['mappingFile']['qnode'], namespace=self.ns), namespace=self.ns)   # a mapping file
                    q.add_statement('P1476', StringValue(v['content']['title']))               # title
                    q.add_statement('P921', StringValue(v['content']['description']))          # described
                    q.add_statement('P127', Item('SDQ1003', namespace=self.ns))  # belongs to
                    q.add_statement('SDP2004', StringValue(', '.join(v['content']['keywords'])), namespace=self.ns)  # keywords
                    q.add_statement('SDP3004', StringValue(v['hashcode']), namespace=self.ns)

                    if self.data['storeColumnValue']:
                        for data in v['content']['variable_measured']:
                            statement = q.add_statement('SDP2005', StringValue(data['column_name']), namespace=self.ns)        # variable measured
                            statement.add_qualifier('SDP2006', StringValue(data['values_of_a_column']), namespace=self.ns)     # the values of a column
                            statement.add_qualifier('SDP2007', Item(data['data_structure_type']), namespace=self.ns)           # data structure type
                            statement.add_qualifier('SDP2008', URLValue(data['semantic_type_identifier']), namespace=self.ns)  # semantic type
                            statement.add_qualifier('P1545', QuantityValue(data['column_index'], namespace=self.ns))           # column index

                doc.kg.add_subject(q)

        return doc

    def model_schema(self):
        # read data
        data = self.read_data(self.data['schema'])

        # initialize KGSchema
        custom_dict, ns_dict = {}, {'wd': 'http://www.wikidata.org/entity/'}
        for each in data['prefix']:
            for k, v in each.items():
                custom_dict[k] = v
                if k != 'wd':
                    ns_dict[k] = v + '/entity'
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        doc = etk.create_document({}, doc_id='http://isi.edu/default-ns/projects')

        # bind prefix
        doc = create_custom_prefix(doc, custom_dict)

        type_map = {
            'quantity': Datatype.QuantityValue,
            'url': URLValue,
            'item': Datatype.Item,
            'time': Datatype.TimeValue,
            'string': Datatype.StringValue,
            'text': Datatype.MonolingualText
        }

        # model schema
        for k, v in data.items():
            if ':' in k:
                k = k.split(':')
                if 'Q' in k[1]:
                    p = WDItem(k[1], namespace=k[0], creator=':datamart')
                elif 'P' in k[1]:
                    p = WDProperty(k[1], type_map[v['type']], namespace=k[0], creator=':datamart')
                else:
                    raise Exception('There is no P/Q information.')
                    return None

                for lang, value in v['description'].items():
                    for val in value:
                        p.add_description(val, lang=lang)

                for lang, value in v['label'].items():
                    for val in value:
                        p.add_label(val, lang=lang)

                for node, value in v['statements'].items():
                    ns = node.split(':')[0] if ':' in node else 'wd'
                    for val in value:
                        prop_type = self.get_property_type(node, ns_dict[ns])
                        if prop_type == 'WikibaseItem':
                            v = Item(str(val['value']))
                        elif prop_type == 'WikibaseProperty':
                            v = Property(val['value'])
                        elif prop_type == 'String':
                            v = StringValue(val['value'])
                        elif prop_type == 'Quantity':
                            v = QuantityValue(val['value'])
                        elif prop_type == 'Url':
                            v = URLValue(val['value'])
                        elif prop_type == 'Monolingualtext':
                            v = MonolingualText(val['value'], val['lang'])
                        p.add_statement(node, v)
                doc.kg.add_subject(p)

        return doc

    def get_property_type(self, node, uri):
        query = """SELECT ?o WHERE { <""" + uri + node + """> wikibase:propertyType ?o .
                   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
                """

        # get Qnodes from wikidata
        def get_results(query):
            sparql = SPARQLWrapper(self.data['satelliteEndpoint'])
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)
            sparql.setRequestMethod(URLENCODED)
            sparql.setQuery(query)
            return sparql.query().convert()['results']['bindings']

        results = get_results(query)
        if results:
            return results[0]['o']['value'].split('#')[-1]
        else:
            return ''


if __name__ == "__main__":
    args = sys.argv
    if '-c' in args:
        config_path = args[args.index('-c') + 1]
    else:
        raise Exception('No config path')
    model = SchemaOfSatelliteDataset(config_path)
    # model statement
    doc = model.model_statement()
    model.write_data(doc, filename='statement.ttl')

    if '-p' in args:
        # run to get property ttl
        doc = model.model_schema()
        model.write_data(doc, 'property.ttl')
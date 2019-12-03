import yaml
import pandas as pd
import json

from etk.knowledge_graph import KGSchema
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.wikidata.entity import WDItem, WDProperty
from etk.wikidata.value import Item, Property, MonolingualText, URLValue, StringValue, Datatype, QuantityValue
from etk.wikidata.__init__ import create_custom_prefix
from hashcode_service import check_files
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED


class SchemaOfSatelliteDataset:
    def __init__(self):
        self.data = {
            'ds': {'existed': False, 'content': {}, 'md5': '', 'qnode': 'SD', 'md_content': ''},
            'wiki': {'existed': False, 'content': None, 'md5': '', 'qnode': 'SD'},
            'mf': {'existed': False, 'content': None, 'md5': '', 'qnode': 'SD'},
            'wiki_class': {'existed': False, 'content': 'Wikifier class', 'qnode': 'SD'},
            'mf_class': {'existed': False, 'content': 'Mapping file class', 'qnode': 'SD'},
            'wd_sat': {'existed': False, 'content': 'Datamart satellite', 'qnode': 'SD'}
        }
        self.sparql_endpoint = 'http://kg2018a.isi.edu:8888/bigdata/namespace/wdq/sparql'

    @staticmethod
    def read_data(file_path):
        # format = file_path.rsplit('.', 1)[1]
        with open(file_path, 'r') as f:
            # if format == 'json':
            #     data = json.loads(f.read())
            # elif format == 'yaml':
            #     data = yaml.safe_load(f)
            # else:
            data = f.read()
        return data

    @staticmethod
    def write_data(doc, file_path, format='ttl'):
        f = open(file_path, 'w')
        f.write(doc.kg.serialize(format))
        print('Serialization completed!')

    @staticmethod
    def init_doc(custom_dict):
        # initialize KGSchema
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        doc = etk.create_document({}, doc_id='http://isi.edu/default-ns/projects')

        # bind prefix
        doc = create_custom_prefix(doc, custom_dict)
        return doc

    def call_qnodes_service(self, files_dict, ns):
        qnodes = check_files(files_dict, ns)
        for k, v in self.data.items():
            if k in qnodes.keys():
                v['qnode'] = v['qnode'] + qnodes[k]['qnode']
                v['md5'] = qnodes[k]['hash_code']
            print(k, v['qnode'])

    def extract_files(self, md_path, ds_path, mf_path, wiki_path):
        # extract metadata and dataset
        md = self.read_data(md_path)
        self.data['ds']['md_content'] = md
        md = json.loads(md)
        self.data['ds']['content'] = {
            'keywords': md['keywords'],
            'description': md['description'],
            'title': md['title'],
            'variable_measured': []
        }
        self.extract_dataset(ds_path)

        # extract wikifier and mapping file
        self.data['wiki']['content'] = self.read_data(wiki_path)
        self.data['mf']['content'] = self.read_data(mf_path)

    def extract_dataset(self, ds_path):
        if ds_path.endswith('.xls'):
            df = pd.read_excel(ds_path, skiprows=[0, 1, 2, 3, 4]).iloc[0:-3, 1:13]
        if ds_path.endswith('.csv'):
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

            self.data['ds']['content']['variable_measured'].append({
                'column_name': df.columns[i],
                'values_of_a_column': values,
                'data_structure_type': data_type,
                'semantic_type_identifier': semantic_type_url,
                'column_index': i
            })

    def model_property(self, prop_path):
        # read data
        data = self.read_data(prop_path)
        data = yaml.safe_load(data)

        # init doc
        custom_dict, ns_dict = {}, {'wd': 'http://www.wikidata.org/entity/'}
        for each in data['prefix']:
            for k, v in each.items():
                custom_dict[k] = v
                if k != 'wd':
                    ns_dict[k] = v + '/entity'
        doc = self.init_doc(custom_dict)

        type_map = {
            'quantity': Datatype.QuantityValue,
            'url': URLValue,
            'item': Datatype.Item,
            'time': Datatype.TimeValue,
            'string': Datatype.StringValue,
            'text': Datatype.MonolingualText
        }

        # model property
        for k, v in data.items():
            if ':' in k:
                k = k.split(':')
                p = WDProperty(k[1], type_map[v['type']], namespace=k[0], creator=':datamart')

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

    def get_property_type(self, node, ns_path):
        query = """SELECT ?o WHERE { <""" + ns_path + node + """> wikibase:propertyType ?o .
                   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
                """

        # get Qnodes from wikidata
        def get_results(query):
            sparql = SPARQLWrapper(self.sparql_endpoint)
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

    def model_statement(self, ns, ns_path):
        # init doc
        doc = self.init_doc(custom_dict={ns: ns_path})

        # call services at first
        files_dict = {}
        for k, v in self.data.items():
            if k == 'ds':
                # use metadata to replace dataset[large]
                files_dict[k] = v['md_content']
            else:
                files_dict[k] = v['content']
        self.call_qnodes_service(files_dict, ns)

        for k, v in self.data.items():
            # construct wikifier instance
            if k == 'wiki' and not v['existed']:
                q = WDItem(v['qnode'], namespace=ns, creator=':datamart')
                q.add_label('A wikifier file of crime data', lang='en')
                q.add_statement('P31', Item(self.data['wiki_class']['qnode'], namespace=ns))  # an instance of Wikifier
                q.add_statement('P127', Item(self.data['wd_sat']['qnode'], namespace=ns))     # belongs to
                q.add_statement('SDP3003', StringValue(v['content']), namespace=ns)           # hasFileContent
                q.add_statement('SDP3004', StringValue(v['md5']), namespace=ns)               # hashValue

            # construct mapping_file instance
            elif k == 'mf' and not v['existed']:
                q = WDItem(v['qnode'], namespace=ns, creator=':datamart')
                q.add_label('A mapping file', lang='en')
                q.add_statement('P31', Item(self.data['mf_class']['qnode'], namespace=ns))  # an instance of MappingFile
                q.add_statement('P170', StringValue('T2WML'))
                q.add_statement('P127', Item(self.data['wd_sat']['qnode'], namespace=ns))
                q.add_statement('SDP3003', StringValue(v['content']), namespace=ns)
                q.add_statement('SDP3004', StringValue(v['md5']), namespace=ns)

            # construct dataset instance
            elif k == 'ds' and not v['existed']:
                q = WDItem(v['qnode'], namespace=ns, creator=':datamart')
                q.add_label(v['content']['title'], lang='en')
                q.add_statement('P31', Item('Q1172284'))  # an instance of Dataset
                q.add_statement('SDP3001', Item(self.data['wiki']['qnode'], namespace=ns), namespace=ns) # a wikifier file
                q.add_statement('SDP3002', Item(self.data['mf']['qnode'], namespace=ns), namespace=ns)   # a mapping file
                q.add_statement('P1476', StringValue(v['content']['title']))               # title
                q.add_statement('P921', StringValue(v['content']['description']))          # described
                q.add_statement('P127', Item(self.data['wd_sat']['qnode'], namespace=ns))  # belongs to
                q.add_statement('SDP2004', StringValue(', '.join(v['content']['keywords'])), namespace=ns)  # keywords

                for data in v['content']['variable_measured']:
                    statement = q.add_statement('SDP2005', StringValue(data['column_name']), namespace=ns)        # variable measured
                    statement.add_qualifier('SDP2006', StringValue(data['values_of_a_column']), namespace=ns)     # the values of a column
                    statement.add_qualifier('SDP2007', Item(data['data_structure_type']), namespace=ns)           # data structure type
                    statement.add_qualifier('SDP2008', URLValue(data['semantic_type_identifier']), namespace=ns)  # semantic type
                    statement.add_qualifier('P1545', QuantityValue(data['column_index'], namespace=ns))           # column index

            # construct normal instance
            elif not v['existed']:
                q = WDItem(v['qnode'], namespace=ns, creator=':datamart')
                q.add_label(v['content'], lang='en')
                q.add_statement('P31', Item('28326484'))

            doc.kg.add_subject(q)
        return doc


if __name__ == "__main__":
    md, ds = 'files/metadata.json', 'files/alabama.xls'
    mf, wiki, prop_path = 'files/fbi.yaml', 'files/wikifier.csv', 'files/properties.yaml'
    prop_output, stmt_output = 'output/property.ttl', 'output/statement.ttl'

    model = SchemaOfSatelliteDataset()

    # run once to get property ttl
    doc = model.model_property(prop_path)
    model.write_data(doc, prop_output)

    # extract data from four files
    model.extract_files(md, ds, mf, wiki)
    doc = model.model_statement(ns='dm', ns_path='https://w3id.org/satellite/dm')
    model.write_data(doc, stmt_output)


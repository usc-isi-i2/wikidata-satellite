import hashlib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED


class Hashcode:
    def __init__(self, endpoint):
        self.sparql_endpoint = endpoint

    def check_hash(self, ns_uri, hashcode):
        prop_uri = '<' + ns_uri + '/prop/SDP3004>/' + "<" + ns_uri + '/prop/statement/SDP3004>'
        query = """SELECT ?s WHERE { ?s """ + prop_uri + " \"" + hashcode + """\"^^xsd:string .
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
            return results[0]['s']['value'].split('/')[-1]
        else:
            return None

    def check_files(self, uri, path):
        with open(path, 'rb') as f:
            content = f.read()
            hashcode = hashlib.md5(content).hexdigest()
        qnode = self.check_hash(uri, hashcode)
        return qnode, hashcode


# if __name__ == "__main__":
#     hc = Hashcode()
#     qnode = hc.check_hash('https://w3id.org/satellite/dm', "07d6436c69fc9a516e1651ddbe40e2ed")
#     print(qnode)

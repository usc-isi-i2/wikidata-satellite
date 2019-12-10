# wikidata-satellite

## Prerequisites

* Python 3.6+

## Installation

1. Open a terminal, clone wikidata-reservation repository at first and run wikidata-reservation service:
    ```
    git clone https://github.com/usc-isi-i2/wikidata-reservation
    cd wikidata-reservation
    pip install -r requirements.txt
    python reservation_rest_api.py
    ```
2. Open another terminal, clone the repository and run these commands
    ```
    git clone https://github.com/usc-isi-i2/wikidata-satellite
    cd wikidata-satellite/satellite
    pip install -r requirements.txt
    python -m spacy download en_core_web_sm
    python dmg.py -c config.yaml -p
    ```
    Arguments: 
    1. -c: path of config.yaml
    2. -p: export schema.ttl, optional parameter

4. In wikidata-satellite/satellite/ouput, you can see the final triples.

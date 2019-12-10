# wikidata-satellite

## Prerequisites

* Python 3.6+

## Installation

1. Open a terminal and clone wikidata-reservation repository at first and run wikidata reservation service:
    ```
    git clone https://github.com/usc-isi-i2/wikidata-reservation
    cd wikidata-reservation
    pip install -r requirements.txt
    python reservation_rest_api.py
    ```
2. Open another terminal and clone the repository
    ```
    git clone https://github.com/usc-isi-i2/wikidata-satellite
    ```
3. Run these commands:
    ```
    cd wikidata-satellite/satellite
    pip install -r requirements.txt
    python dmg.py -c config.yaml [-p]
    ```
4. In ouput path, you can see the final triples.

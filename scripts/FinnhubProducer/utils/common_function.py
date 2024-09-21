import io

import finnhub
import avro.schema
import avro.io

def load_client(api_key: [str, None]):
    if api_key is None:
        raise ValueError("API Key is required")
    return finnhub.Client(api_key)

def validate_symbol(client, symbol: str):
    lookup_result = client.symbol_lookup(symbol)
    for record in lookup_result["result"]:
        if record["symbol"] == symbol:
            return record
    return None

def load_schema(schema_path: str):
    return avro.schema.parse(open(schema_path).read())

def serialize_avro(record: dict, schema: avro.schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(record, encoder)
    return bytes_writer.getvalue()
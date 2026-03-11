

from rx import create
from rx import operators as ops


def kafka_observable(consumer):
    def _observable(observer, _):
        try:
            for msg in consumer:
                observer.on_next(msg.value)
        except Exception as e:
            observer.on_error(e)
    return create(_observable)

def machine_codes():
    return {"UNS56A": "UNSCRAMBLER",
        "WS964F": "WASHER",
        "IS8710": "INSPECTION",
        "FB713A": "FILLING",
        "C7841R": "CARBONATOR",
        "CPM784": "CAPPING",
        "LBL74F": "LABELING",
        "PLL741": "PALLETIZER"
    }

def machines_mapping(code):
    return machine_codes()[code]


def machine_properties():
    return {
        'A7': 'LITERS',
        'W8': 'QUALITY',
        'L1': 'LIGHT',
        'T3': 'TIME',
        'P6': 'POWER',
        'G8': 'GRADES'
    }

def properties_mapping(code):
    return machine_properties()[code]

def machine_attributes():
    return {
        "TS": "TIMESTAMP",
        "MC": "MACHINE",
        "PR": "PRODUCT",
        "PS": "PROPS",
    }

def attributes_mapping(code):
    return machine_attributes()[code]

def auxa(event):
    print(f"a: {event}")

def auxb(event):
    print(f"b: {event}")

def aux(e):
    print(e['PS'].keys())
    e['PS'] = {machine_properties()[k]:e['PS'][k] for k in e['PS'].keys()}
    e = {attributes_mapping(k):e[k] for k in e.keys()}
    e['MACHINE'] = machine_codes()[e['MACHINE']]
    return e

def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(
        ops.do_action(save_raw_event),
        ops.do_action(lambda e: print(f"event received: {e}")),
        ops.filter(lambda e: e["MC"] in machine_codes().keys()),
        ops.map(aux),
        ops.do_action(send_rich_event),
        ops.do_action(lambda e: print("rich event send", e)),
        ops.do_action(save_rich_event)
    )
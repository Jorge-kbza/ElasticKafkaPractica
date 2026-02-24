

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

def properties_mapping(code):
    # TODO
    pass

def attributes_mapping(code):
    # TODO
    pass

def auxa(event):
    print(f"a: {event}")

def auxb(event):
    print(f"b: {event}")

def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(
        ops.do_action(lambda e: print(f"event received: {e}")),
        # TODO
        ops.do_action(lambda _: print("rich event send"))
    )
import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

def run(event, context):
    iterator = event['iterator']
    index = iterator['index']
    step = iterator['step']
    count = iterator['count']

    index += step

    return  {
        "index": index,
        "step": step,
        "count": count,
        "continue": index < count
    }

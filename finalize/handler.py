import os
import sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from finalizeStep import finalizeStep

def run(event, context):
    print("run")
    step = finalizeStep(event.get("id"), {})
    step.run()

    return event

# python -c 'from handler import *; test()'
def test():
    data = { "id": "example"}
    print(run(data, {}))

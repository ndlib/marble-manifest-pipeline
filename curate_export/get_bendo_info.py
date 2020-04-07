# get_bendo_info.py
# import _set_path  # noqa: F401
import dependencies.requests
from dependencies.sentry_sdk import capture_exception


def get_bendo_info(bendo_base_url, bendo_item, filename):
    results = {}
    url = bendo_base_url + "/item/" + bendo_item + "/" + filename
    try:
        response = dependencies.requests.head(url)  # , headers=self.curate_header)
        if response:
            results = response.headers
    except dependencies.requests.exceptions.InvalidURL as e:
        print("invalid url in get_bendo_info: ", url)
        capture_exception(e)
    except ConnectionRefusedError as e:
        print('Connection refused in get_bendo_info on url ', url)
        capture_exception(e)
    except Exception as e:  # noqa E722 - intentionally ignore warning about bare except
        print('Error caught in get_bendo_info trying to process url ' + url)
        capture_exception(e)
    return results


# export SSM_KEY_BASE=/all/new-csv
# aws-vault exec testlibnd-superAdmin --session-ttl=1h --assume-role-ttl=1h --
# python -c 'from get_bendo_info import *; test()'
def test(identifier=""):
    """ test exection """
    bendo_base_url = "http://bendo-pprd-vm.library.nd.edu:14000"
    bendo_base_url = "http://bendo-prod-vm.library.nd.edu:14000"
    bendo_item = "0g354f1907g"
    filename = 'October197613thissue.pdf'
    results = get_bendo_info(bendo_base_url, bendo_item, filename)
    print("results = ", results)

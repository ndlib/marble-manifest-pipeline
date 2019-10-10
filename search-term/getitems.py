from conaiagen import get_conaia_list
from locgen import get_loc_list
from termgen import get_term_list


def get_list(uri):
    if("http://www.getty.edu/cona/" in uri):
        getresponse = get_conaia_list(uri)
    elif("http://www.loc.gov/item/" in uri):
        getresponse = get_loc_list(uri)
    elif("vocab.getty.edu/" in uri):
        getresponse = get_term_list(uri)
    else:
        getresponse = "Unable to parse URI"
    return getresponse

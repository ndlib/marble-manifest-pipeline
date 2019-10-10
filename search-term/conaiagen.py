import urllib2
import re


def get_conaia_list(conaiauri):
    response = urllib2.urlopen("http://www.getty.edu/cona/CONAIconographyRecord.aspx?iconid=901000161")
    lines = response.readlines()
    result = ""
    for line in lines:
        result += line
    result = result.split('<div class="page">', 1)[-1]
    result = result.lower()
    result = re.sub("<br>", "; ", result)
    result = re.sub("</td>", "; ", result)
    result = re.sub("[<].*?[>]", "", result)
    result = re.sub("[\n\r\t]", "", result)
    result = re.sub("; ;", ";", result)
    result = re.sub("  ", "", result)
    results = result.split("; ")
    return results

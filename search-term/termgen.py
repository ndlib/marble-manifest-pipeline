import urllib2
import re


def getTermList(termid):
    response = urllib2.urlopen('http://www.getty.edu/vow/AATHierarchy?find=&logic=AND&note=&page=1&subjectid='+termid)
    lines = response.readlines()
    result = ""
    for line in lines:
        if 'http://www.getty.edu/vow/' in line:
            result += line
    # take results from hierarchy page and return a csv list
    result = re.sub("[\".\&\(\[\<].*?[;\)\]\>]", "", result)
    result = re.sub("\n\n", "\n", result)
    result = re.sub("\n", ", ", result)
    return result

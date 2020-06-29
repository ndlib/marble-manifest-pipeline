Curate Export Process

When exporting from Curate, we start with a list of ids to export, like this: ["und:zp38w953h0s", "und:zp38w953p3c"]
For each id, we do the following:
1.  Make a call to the Curate API for this item:  https://curate.nd.edu/api/items/zp38w953h0s
2.  Get the "membersUrl" from the above call to get a list of all members:  https://curate.nd.edu/api/items?part_of=zp38w953h0s&rows=100
    Note that this is limited to 100 rows of results for each call, so we loop through each of the result nodes
      collecting the item url for later retrieval.
    As long as there is a nextPage node, we continue to the next page and repeat:  https://curate.nd.edu/api/items?page=2&part_of=zp38w953h0s&rows=10
    Note:  Because datasets timeout when trying to retrieve the url, we skip all records whose "type" is "Dataset".
3.  We retrieve metadata for each item using the urls collected above.  e.g. https://curate.nd.edu/api/items/pv63fx74g23

Challenges:
1.  Inconsistency in metadata names and data types (string vs array).
  https://curate.nd.edu/api/items/pv63fx74g23 has:
    "creator#author": "University of Notre Dame",
    "contributor": "Sorin, Edward",
  https://curate.nd.edu/api/items/zp38w953p3c has:
    "creator": [
    "王肅達 Wang Suda, 1910-1963 [creator]",
    "Thomas M. Megan, S.V.D., 1899-1951 [commissioner]"
    ]
    "contributor#curator": "Hye-jin Juhn, East Asian Studies Librarian"
1b. Here is a link to a spreadsheet detailing metadata in Curate: https://docs.google.com/spreadsheets/d/1uLQhOclIGuaS-pmOyPYmIWTTDxtgDyiWQpdhlSI3TKQ/edit#gid=878919532
2.  Speed - trying to follow this process for the Architectural Lantern Slides (und:qz20sq9094h),
      I projected that it would take 9.3 hours to harvest the metadata.
3.  A combination of json and xml.  Most metadata is in json format, with the exception of "characterization" (associated with "containedFiles").
    Retrieving the md5Checksum requires parsing xml.
4.  The md5Checksum stored in Bendo is base64 encoded, and must be decoded in order to get the actual md5Checksum.

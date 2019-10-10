#Term Generation

When calling on get_list, supply the Art and Architecture Thesaurus(AAT) URI for hierarchy page, the CONA Iconography URI, or the Library of Congress Item URI. get_list will then determine which URI type it is, call to have the URI processed, and return the usable data.

#Example Use

Using the ID for marble (rock)

`get_list('http://www.getty.edu/vow/AATHierarchy?find=&logic=AND&note=&page=1&subjectid=300011443')`

expected response:

`[Top of the AAT hierarchies, Materials Facet, Materials , materials , materials by composition, materials by composition, inorganic material, rock , metamorphic rock, marble , marble by composition or origin, marble by composition or origin, Botticino marble, Candoglia marble, dolomitic marble, Georgia marble, Mondragone marble, Purbeck marble, marble by form or function, marble by form or function, breccia marble, madrepore marble, shell marble, statuary marble, marble by color or pattern, marble by color or pattern, black marble, gray marble, green marble, onyx marble, pink marble, red marble, variegated marble, white marble, yellow marble]`

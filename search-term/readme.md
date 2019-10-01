#Term Generation

When calling on getTermList, supply the Art and Architecture Thesaurus(AAT) ID

Function takes the id and retrieves the hierarchy listing of terms, including all parent terms and two levels of children terms. After removing all extra formatting and html tags, function compiles the hierarchy list as a csv list.

#Example Use


Using the ID for marble (rock)

`getTermList(300011443)`

expected response:

`[Top of the AAT hierarchies, Materials Facet, Materials , materials , materials by composition, materials by composition, inorganic material, rock , metamorphic rock, marble , marble by composition or origin, marble by composition or origin, Botticino marble, Candoglia marble, dolomitic marble, Georgia marble, Mondragone marble, Purbeck marble, marble by form or function, marble by form or function, breccia marble, madrepore marble, shell marble, statuary marble, marble by color or pattern, marble by color or pattern, black marble, gray marble, green marble, onyx marble, pink marble, red marble, variegated marble, white marble, yellow marble]`

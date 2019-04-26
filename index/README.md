# marble_index

The main purpose for this project is to create index records to facilitate search.
Index records will be copied to an S3 bucket, and then copied on to a local server to ingest the index.


## Usage:

When calling this directly, call passing the Unique ID or the URL to the manifest to be indexed.
Examples include:

Collections look like this:  https://marble.library.nd.edu/collection/dante?page=1&perpage=12&terms=dante&ref=search
Items look like this: https://marble.library.nd.edu/item/ils-000909884?ref=collection&id=dante


### Modules
    index_manifest.py
        This module first retrieves manifest information, then tries to find an existing index.
        If an index record is found, the code modifies a copy of it to be searchable through Marble.
        If an index record is not found, the code creates an index record.
        The new index record is written to disk for ingesting into the indexing engine.
        Modules called:
            get_manifest_info.py
            get_existing_index_record.py
            modify_existing_index_record.py
            create_new_index_record.py
            write_index_file.py

    get_manifest_info.py
        This module attempts to retrieve information from an existing manifest.

    get_existing_index_record.py
        This module attempts to retrieve an existing index record, first from production, then from the sandbox

    modify_existing_index_record.py
        If an existing index record is found, this module modifies a copy of that record to include fields
            which are required for searching through Marble.

    create_new_index_record.py
        If an existing index record is not found, this module creates a simple index record to facilitate search.

    write_index_file.py
        Regardless of the source of the new index record, this module writes it to disk.
        Modules called:
            file_system_utilities.py

    file_system_utilities.py
        This module performs the actual writing to disk.



## To locally copy index records, run: ./copy_pnx_to_Primo.sh to copy PNX records into Primo


To test, cd into the test directory, and run:
    python run_all_tests.py

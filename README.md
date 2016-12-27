# snapshot

Description
X  create sample tables and data.
  define snapshot subset
    table snapshot: table-name, column-names, args, clauses
      {:table "table_name" :columns ["column_names"] :condition {:sql-where}}
    db snapshot: table snapshots

  snapshot: select subset of data and optionally persist in edn/csv?
  load snapshot: read from file, into tables
*  diff snapshots:
specs and tests
web view

## Installation

Download from http://example.com/FIXME.

## Usage

FIXME: explanation

    $ java -jar snapshot-0.1.0-standalone.jar [args]

## Options

FIXME: listing of options this app accepts.

## Examples

...

### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

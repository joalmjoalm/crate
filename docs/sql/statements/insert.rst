.. highlight:: psql
.. _ref-insert:

==========
``INSERT``
==========

Create new rows in a table.

.. rubric:: Table of contents

.. contents::
   :local:

.. _insert_synopsis:

Synopsis
========

::

    INSERT INTO table_ident
      [ ( column_ident [, ...] ) ]
      { VALUES ( expression [, ...] ) [, ...] | ( query ) | query }
      [ ON CONFLICT (column_ident [, ...]) DO UPDATE SET { column_ident = expression [, ...] } |
        ON CONFLICT [ ( column_ident [, ...] ) ] DO NOTHING ]

Description
===========

INSERT creates one or more rows specified by value expressions.

The target column names can be listed in any order. If no list of column names
is given at all, the default is all the columns of the table in lexical order;
or the first N column names, if there are only N columns supplied by the VALUES
clause. The values supplied by the VALUES clause are associated with the
explicit or implicit column list left-to-right.

Each column not present in the explicit or implicit column list will not be
filled.

If the expression for any column is not of the correct data type, automatic
type conversion will be attempted.


``ON CONFLICT DO UPDATE SET``
-----------------------------

This clause can be used to update a record if a conflicting record is
encountered.

::

     ON CONFLICT (conflict_target) DO UPDATE SET { assignments }

     WHERE

      conflict_target := column_ident [, ... ]
      assignments := column_ident = expression [, ... ]


Within expressions in the ``DO UPDATE SET`` clause, you can use the special
``excluded`` table to refer to column values from the INSERT statement values.
For example:

::

     INSERT INTO t (col1, col2) VALUES (1, 41)
     ON CONFLICT (col1) DO UPDATE SET col2 = excluded.col2 + 1

The above statement would update ``col2`` to ``42`` if ``col1`` was a primary
key and the value ``1`` already existed for ``col1``.

``ON CONFLICT DO NOTHING``
--------------------------

When ``ON CONFLICT DO NOTHING`` is specified, rows which caused a duplicate
key conflict will not be inserted. No exception will be thrown. For example:

::

     INSERT INTO t (col1, col2) VALUES (1, 42)
     ON CONFLICT DO NOTHING

In the above statement, if ``col1`` had a primary key constraint and the value
``1`` already existed for ``col1``, no insert would be performed. The conflict
target after ``ON CONFLICT`` is optional.

Insert from dynamic queries constraints
---------------------------------------

In some cases ``SELECT`` statements produce invalid data. This opens a rare
occasion for inconsistent outcomes. If the select statement produces data where
a few rows contain invalid column names, or where you have rows which types are
not compatible among themselves, some rows will be inserted while others will
fail. In this case the errors are logged on the node. This could happen in the
following cases:

  * If you select invalid columns or incompatible data types with unnest
    e.g.::

        select unnest([{foo=2}, {foo='a string'}])

    or::

        select unnest([{_invalid_col='foo', valid_col='bar'}])

  * If you select from an ignored object which contains different data
    types for the same object column, e.g.::

        insert into from_table (o) values ({col='foo'}),({col=1})
        insert into to_table (i) (select o['col'] from t)

Any updates which happened before the failure will be persisted, which will
lead to inconsistent outcomes. So special care needs to be taken by the
application when using statements which might produce dynamic data.

Parameters
==========

:table_ident:
  The identifier (optionally schema-qualified) of an existing table.

:column_ident:
  The name of a column or field in the table pointed to by *table_ident*.

:expression:
  An expression or value to assign to the corresponding column.

:query:
  A query (SELECT statement) that supplies the rows to be inserted.
  Refer to the ``SELECT`` statement for a description of the syntax.

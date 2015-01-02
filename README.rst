nfldb-projections
=================

**Note: work in progress. Not yet functional.**

nfldb-projections is an add-on to the `nfldb`_ library.
Whereas nfldb manages NFL statistics, nfldb-projections manages projections of those statistics,
with a particular eye toward fantasy projections.
nfldb-projections can also keep track of player salaries from DFS sites.

New tables
----------

The following tables are added to the nfldb database:

* **projection_source** stores a row for each source of projections (i.e., website).
* **projection_set** stores a row for each set of projections,
  representing a specific data-access event.
* **fp_system** stores a row for each fantasy-points system targeted by a projection.
  Many websites do not post projections in terms of the statistics collected by nfldb, but rather in terms of fantasy points.
  As there are multiple fantasy-points systems, it is important to keep track of which system was used by each projection.
  Note that nfldb-projections does not (currently) calculate fantasy points from raw statistics,
  it merely keeps track of the system that each data source is supposed to apply to.
* **dfs_site** stores a row for every daily fantasy site.
* **dfs_salary** stores a row for each player each week, keeping track of the player salaries.
* **stat_projection** stores projections of the statistics collected by nfldb.
  Each row corresponds to a unique player, a unique game, and a unique projection set.
  Otherwise, this table has the same columns as the ``agg_play`` table of nfldb.
* **fp_projection** stores fantasy-point projections.
  Each row corresponds to a unique player, a unique game, a unique projection set, and a unique fantasy-point system.
* **name_disambiguation** stores the ``player_id`` for names that cannot be found in the player table.
  Rows can be added with the ``add_name_disambiguations`` function.


Entity-relationship diagram
---------------------------

* `condensed`_

.. _nfldb: https://github.com/BurntSushi/nfldb
.. _condensed: https://github.com/hsharrison/nfldb-projections/raw/master/nfldb-projections-erd.pdf

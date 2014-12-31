nfldb-projections
=================

**Note: work in progress. Not yet functional.**

nfldb-projections is an add-on to the `nfldb`_ library.
Whereas nfldb manages NFL statistics, nfldb-projections manages projections of those statistics,
with a particular eye toward fantasy projections.

New tables
----------

The following tables are added to the nfldb database:

* **projection_source** stores a row for each source of projections (i.e., website).
* **projection_set** stores a row for each set of projections.
  It contains information such as when the data was scraped.
* **fp_system** stores a row for each fantasy-points system targeted by a projection.
  Many websites do not post projections in terms of the statistics collected by nfldb, but rather in terms of fantasy points.
  As there are multiple fantasy-points systems, it is important to keep track of which system was used by each projection.
  Note that nfldb-projections does not (currently) calculate fantasy points from raw statistics,
  it merely keeps track of the system that each data source is supposed to apply to.
* **projected_stats** stores projections of the statistics collected by nfldb.
  Each row corresponds to a unique player, a unique game, and a unique projection set.
  Otherwise, this table has the same columns as the ``play_player`` table of nfldb.
* **projected_fp** stores fantasy-point projections.
  Each row corresponds to a unique player, a unique game, a unique projection set, and a unique fantasy-point system.

.. _nfldb: https://github.com/BurntSushi/nfldb

SELECT MIN(t.title) AS movie_title
FROM db2.keyword AS k,
     db1.movie_info AS mi,
     db2.movie_keyword AS mk,
     db1.title AS t
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Bulgaria')
  AND t.production_year > 2010
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id


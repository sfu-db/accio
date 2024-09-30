SELECT MIN(mi_idx.info) AS rating,
       MIN(t.title) AS movie_title
FROM db2.info_type AS it,
     db1.keyword AS k,
     db1.movie_info_idx AS mi_idx,
     db1.movie_keyword AS mk,
     db1.title AS t
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '9.0'
  AND t.production_year > 2010
  AND t.id = mi_idx.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.id = mk.keyword_id
  AND it.id = mi_idx.info_type_id


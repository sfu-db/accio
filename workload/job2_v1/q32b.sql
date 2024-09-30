SELECT MIN(lt.link) AS link_type,
       MIN(t1.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM db1.keyword AS k,
     db1.link_type AS lt,
     db1.movie_keyword AS mk,
     db1.movie_link AS ml,
     db1.title AS t1,
     db1.title AS t2
WHERE k.keyword ='character-name-in-title'
  AND mk.keyword_id = k.id
  AND t1.id = mk.movie_id
  AND ml.movie_id = t1.id
  AND ml.linked_movie_id = t2.id
  AND lt.id = ml.link_type_id
  AND mk.movie_id = t1.id


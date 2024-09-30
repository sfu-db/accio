SELECT MIN(n.name) AS member_in_charnamed_movie,
       MIN(n.name) AS a1
FROM db1.cast_info AS ci,
     db2.company_name AS cn,
     db2.keyword AS k,
     db2.movie_companies AS mc,
     db2.movie_keyword AS mk,
     db1.name AS n,
     db1.title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE 'Z%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id


SELECT MIN(n.name) AS member_in_charnamed_movie
FROM db2.cast_info AS ci,
     db1.company_name AS cn,
     db1.keyword AS k,
     db1.movie_companies AS mc,
     db1.movie_keyword AS mk,
     db2.name AS n,
     db1.title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id


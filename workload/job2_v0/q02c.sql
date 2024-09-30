SELECT MIN(t.title) AS movie_title
FROM db2.company_name AS cn,
     db2.keyword AS k,
     db2.movie_companies AS mc,
     db2.movie_keyword AS mk,
     db1.title AS t
WHERE cn.country_code ='[sm]'
  AND k.keyword ='character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id


SELECT MIN(t.title) AS movie_title
FROM db1.company_name AS cn,
     db1.keyword AS k,
     db1.movie_companies AS mc,
     db1.movie_keyword AS mk,
     db1.title AS t
WHERE cn.country_code ='[nl]'
  AND k.keyword ='character-name-in-title'
  AND cn.id = mc.company_id
  AND mc.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND mc.movie_id = mk.movie_id


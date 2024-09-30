SELECT MIN(atl.title) AS aka_title,
       MIN(t.title) AS internet_movie_title
FROM db1.aka_title AS atl,
     db1.company_name AS cn,
     db1.company_type AS ct,
     db2.info_type AS it1,
     db1.keyword AS k,
     db1.movie_companies AS mc,
     db1.movie_info AS mi,
     db1.movie_keyword AS mk,
     db1.title AS t
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mi.note LIKE '%internet%'
  AND t.production_year > 1990
  AND t.id = atl.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = atl.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = atl.movie_id
  AND mc.movie_id = atl.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id


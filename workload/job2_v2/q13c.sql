SELECT MIN(cn.name) AS producing_company,
       MIN(miidx.info) AS rating,
       MIN(t.title) AS movie_about_winning
FROM db2.company_name AS cn,
     db2.company_type AS ct,
     db2.info_type AS it,
     db2.info_type AS it2,
     db2.kind_type AS kt,
     db2.movie_companies AS mc,
     db1.movie_info AS mi,
     db1.movie_info_idx AS miidx,
     db1.title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND t.title <> ''
  AND (t.title LIKE 'Champion%'
       OR t.title LIKE 'Loser%')
  AND mi.movie_id = t.id
  AND it2.id = mi.info_type_id
  AND kt.id = t.kind_id
  AND mc.movie_id = t.id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND miidx.movie_id = t.id
  AND it.id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id


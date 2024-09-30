SELECT MIN(chn.name) AS character_name,
       MIN(t.title) AS russian_mov_with_actor_producer
FROM db1.char_name AS chn,
     db1.cast_info AS ci,
     db2.company_name AS cn,
     db2.company_type AS ct,
     db2.movie_companies AS mc,
     db1.role_type AS rt,
     db1.title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[ru]'
  AND rt.role = 'actor'
  AND t.production_year > 2010
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id


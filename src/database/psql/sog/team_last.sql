WITH last_n_games AS (
  SELECT 
    tid, 
    gid, 
    sog
  FROM (
    SELECT 
      tid, 
      gid, 
      sog, 
      ROW_NUMBER() OVER (PARTITION BY tid ORDER BY gid DESC) AS rn
    FROM 
      team_stat
    ) sub
    WHERE 
      rn <= :last_n 
)

SELECT 
  t.tid, 
  t.abbr, 
  ROUND(AVG(ls.sog), 2) AS avg_last_n_games
FROM 
  team t
JOIN 
  last_n_games ls ON 
  t.tid = ls.tid
GROUP BY 
  t.tid, 
  t.abbr
ORDER BY 
  avg_last_n_games DESC;
  

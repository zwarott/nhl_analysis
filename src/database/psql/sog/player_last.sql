WITH last_n_games AS (
  SELECT 
    pid,
    gid,
    sog
  FROM (
    SELECT 
      pid, 
      gid, 
      sog, 
      ROW_NUMBER() OVER (PARTITION BY pid ORDER BY gid DESC) AS rn
    FROM 
      skater_stat
    WHERE 
      tid = :team_id 
    ) sub
      WHERE 
        rn <= :last_n 
)
SELECT 
  p.pid, 
  p.name, 
  ROUND(avg(ls.sog), 2) AS avg_last_n_games
FROM 
  player p
JOIN 
  last_n_games ls ON 
  p.pid = ls.pid
GROUP BY 
  p.pid, 
  p.name
ORDER BY avg_last_n_games DESC;

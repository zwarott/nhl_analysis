WITH last_n_home_games AS (
    SELECT 
      htid AS tid,
      gid
    FROM (
      SELECT 
        htid, 
        gid, 
        ROW_NUMBER() OVER (PARTITION BY htid ORDER BY gid DESC) AS rn
      FROM 
        game
      WHERE 
        htid IS NOT NULL
    ) sub
    WHERE 
      rn <= :last_n 
)

SELECT 
  t.tid, 
  t.abbr, 
  ROUND(AVG(ts.sog), 2) AS avg_last_n_home_games
FROM 
  team t
JOIN 
  last_n_home_games lg ON 
  t.tid = lg.tid
JOIN 
  team_stat ts ON 
  lg.gid = ts.gid AND 
  lg.tid = ts.tid
GROUP BY 
  t.tid, 
  t.abbr
ORDER BY 
  avg_last_n_home_games DESC;

WITH last_n_home_games AS (
    SELECT 
        htid AS tid
    FROM (
        SELECT 
            htid, 
            ROW_NUMBER() OVER (PARTITION BY htid ORDER BY gid DESC) AS rn
        FROM 
          game
        WHERE 
          htid IS NOT NULL
    ) sub
    WHERE 
      rn <= :last_n
),
last_n_away_games AS (
    SELECT 
        atid AS tid
    FROM (
      SELECT 
        atid, 
        ROW_NUMBER() OVER (PARTITION BY atid ORDER BY gid DESC) AS rn
      FROM 
        game
      WHERE 
        atid IS NOT NULL
    ) sub
    WHERE 
      rn <= :last_n 
)
SELECT 
    t.tid, 
    t.abbr, 
    ROUND(AVG(CASE WHEN lg.tid IS NOT NULL THEN ts.sog END), 2) AS avg_last_n_home_games,
    ROUND(AVG(CASE WHEN la.tid IS NOT NULL THEN ts.sog END), 2) AS avg_last_n_away_games
FROM 
    team t
LEFT JOIN 
    last_n_home_games lg ON 
    t.tid = lg.tid
LEFT JOIN 
    last_n_away_games la ON 
    t.tid = la.tid
LEFT JOIN 
    team_stat ts ON 
    lg.tid = ts.tid OR 
    la.tid = ts.tid
GROUP BY 
    t.tid, 
    t.abbr
ORDER BY 
    avg_last_n_home_games DESC;

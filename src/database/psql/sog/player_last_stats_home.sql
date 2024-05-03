WITH last_n_home_games AS (
    SELECT 
        s.pid, 
        s.gid, 
        s.sog,
        s.pts,
        s.g, 
        s.ppg,
        s.a, 
        s.ppa,
        s.pm,
        ROW_NUMBER() OVER (PARTITION BY s.pid ORDER BY g.date DESC) AS rn
    FROM 
        skater_stat s
    JOIN 
        game g ON s.gid = g.gid AND g.htid = :team_id  -- Filter for home games of the selected team
    ORDER BY 
        g.date DESC
),
overall_stats AS (
    SELECT 
        p.pid, 
        p.name, 
        ROUND(avg(s.sog), 2) AS sog_avg,
        MIN(s.sog) AS min_sog,
        MAX(s.sog) AS max_sog,
        ROUND(avg(s.pts), 2) AS pts_avg,
        ROUND(avg(s.g), 2) AS g_avg,
        ROUND(avg(s.ppg), 2) AS ppg_avg,
        ROUND(avg(s.a), 2) AS a_avg,
        ROUND(avg(s.ppa), 2) AS ppa_avg,
        ROUND(avg(s.pm), 2) AS pm_avg
    FROM 
        player p
    JOIN 
        last_n_home_games s ON p.pid = s.pid
    WHERE 
        s.rn <= :last_n  -- Filter for the last n home games
    GROUP BY 
        p.pid, 
        p.name
)
SELECT * FROM overall_stats
ORDER BY sog_avg DESC;

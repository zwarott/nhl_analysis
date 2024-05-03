WITH last_n_away_games AS (
    SELECT 
        s.pid, 
        s.tid,
        s.gid, 
        s.sog,
        s.pts,
        s.g, 
        s.ppg,
        s.a, 
        s.ppa,
        s.pm
    FROM (
        SELECT 
            sk.pid, 
            sk.tid,
            sk.gid, 
            sk.sog,
            sk.pts,
            sk.g, 
            sk.ppg,
            sk.a, 
            sk.ppa,
            sk.pm,
            ROW_NUMBER() OVER (PARTITION BY sk.tid ORDER BY sk.gid DESC) AS rn
        FROM 
            skater_stat sk
        JOIN 
            game g ON sk.gid = g.gid
        WHERE 
            g.atid = sk.tid -- Filter for away games
    ) AS s
    WHERE 
        rn <= :last_n
),
overall_stats AS (
    SELECT 
        p.pid, 
        p.tid,
        p.name, 
        ROUND(avg(ls.sog), 2) AS sog_avg,
        MIN(ls.sog) AS min_sog,
        MAX(ls.sog) AS max_sog,
        mode() WITHIN GROUP (ORDER BY ls.sog) AS mode_sog,
        ROUND(avg(ls.pts), 2) AS pts_avg,
        MIN(ls.pts) AS min_pts,
        mode() WITHIN GROUP (ORDER BY ls.pts) AS mode_pts,
        ROUND(avg(ls.g), 2) AS g_avg,
        mode() WITHIN GROUP (ORDER BY ls.g) AS mode_g,
        ROUND(avg(ls.ppg), 2) AS ppg_avg,
        mode() WITHIN GROUP (ORDER BY ls.ppg) AS mode_ppg,
        ROUND(avg(ls.a), 2) AS a_avg,
        mode() WITHIN GROUP (ORDER BY ls.a) AS mode_a,
        ROUND(avg(ls.ppa), 2) AS ppa_avg,
        mode() WITHIN GROUP (ORDER BY ls.ppa) AS mode_ppa,
        ROUND(avg(ls.pm), 2) AS pm_avg,
        mode() WITHIN GROUP (ORDER BY ls.pm) AS mode_pm
    FROM 
        player p
    JOIN 
        last_n_away_games ls ON p.pid = ls.pid
    GROUP BY 
        p.pid, 
        p.tid,
        p.name
)
SELECT * FROM overall_stats
ORDER BY :order_by DESC;

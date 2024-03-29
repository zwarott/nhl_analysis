WITH last_5_games AS (
    SELECT 
      pid, 
      gid, 
      sog,
      pts,
      g, 
      ppg,
      a, 
      ppa,
      pm,
      FIRST_VALUE(sog) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_sog,
      FIRST_VALUE(pts) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_pts,
      FIRST_VALUE(g) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_g,
      FIRST_VALUE(ppg) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_ppg,
      FIRST_VALUE(a) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_a,
      FIRST_VALUE(ppa) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_ppa,
      FIRST_VALUE(pm) OVER (PARTITION BY pid ORDER BY COUNT(*) DESC) AS mode_pm
    FROM (
      SELECT 
        pid, 
        gid,
        sog,
        pts, 
        g, 
        ppg, 
        a, 
        ppa,
        pm, 
        ROW_NUMBER() OVER (PARTITION BY pid ORDER BY gid DESC) AS rn
      FROM 
        skater_stat
      WHERE 
        tid = :team_id 
    ) sub
    WHERE 
      rn <= :last_n 
    GROUP BY 
      pid, 
      gid,
      sog, 
      pts, 
      g, 
      ppg,
      a, 
      ppa,
      pm
),
overall_stats AS (
    SELECT 
      p.pid, 
      p.name, 
      ROUND(avg(ls.sog), 2) AS sog_avg,
      ls.mode_sog,
      MIN(ls.sog) AS min_sog,
      MAX(ls.sog) AS max_sog,
      ROUND(avg(ls.pts), 2) AS pts_avg,
      ls.mode_pts,
      ROUND(avg(ls.g), 2) AS g_avg,
      ls.mode_g,
      ROUND(avg(ls.ppg), 2) AS ppg_avg,
      ls.mode_ppg,
      ROUND(avg(ls.a), 2) AS a_avg,
      ls.mode_a,
      ROUND(avg(ls.ppa), 2) AS ppa_avg,
      ls.mode_ppa,
      ROUND(avg(ls.pm), 2) AS pm_avg,
      ls.mode_pm
    FROM 
      player p
    JOIN 
      last_5_games ls ON 
      p.pid = ls.pid
    GROUP BY 
      p.pid, 
      p.name, 
      ls.mode_sog,
      ls.mode_pts, 
      ls.mode_g, 
      ls.mode_ppg, 
      ls.mode_a, 
      ls.mode_ppa,
      ls.mode_pm
)
SELECT * FROM overall_stats
ORDER BY sog_avg DESC;

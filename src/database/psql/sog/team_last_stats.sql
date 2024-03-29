WITH last_n_games AS (
  SELECT 
    tid,
    gid, 
    sog, 
    sp, 
    g, 
    ppg, 
    pim,
    FIRST_VALUE(sog) OVER (PARTITION BY tid ORDER BY COUNT(*) DESC) AS mode_sog,
    FIRST_VALUE(sp) OVER (PARTITION BY tid ORDER BY COUNT(*) DESC) AS mode_sp,
    FIRST_VALUE(g) OVER (PARTITION BY tid ORDER BY COUNT(*) DESC) AS mode_g,
    FIRST_VALUE(ppg) OVER (PARTITION BY tid ORDER BY COUNT(*) DESC) AS mode_ppg,
    FIRST_VALUE(pim) OVER (PARTITION BY tid ORDER BY COUNT(*) DESC) AS mode_pim
  FROM (
    SELECT 
      tid, 
      gid, 
      sog, 
      sp, 
      g, 
      ppg, 
      pim, 
      ROW_NUMBER() OVER (PARTITION BY tid ORDER BY gid DESC) AS rn
    FROM 
      team_stat
    WHERE 
      tid IS NOT NULL
    ) sub
      WHERE 
        rn <= :last_n
      GROUP BY 
        tid, 
        gid, 
        sog, 
        sp, 
        g, 
        ppg, 
        pim
),
overall_stats AS (
    SELECT 
      t.tid, 
      t.abbr, 
      ROUND(avg(lg.sog), 2) AS sog_avg,
      lg.mode_sog,
      MIN(lg.sog) AS min_sog,
      MAX(lg.sog) AS max_sog,
      avg(lg.sp) AS sp_avg,
      lg.mode_sp,
      ROUND(avg(lg.g), 2) AS g_avg,
      lg.mode_g,
      ROUND(avg(lg.ppg), 2) AS ppg_avg,
      lg.mode_ppg,
      ROUND(avg(lg.pim), 2) AS pim_avg,
      lg.mode_pim
    FROM 
      team t
    JOIN 
      last_n_games lg ON 
      t.tid = lg.tid
    JOIN 
      team_stat ts ON 
      lg.gid = ts.gid AND 
      lg.tid = ts.tid
    GROUP BY 
      t.tid, 
      t.abbr, 
      lg.mode_sog, 
      lg.mode_sp, 
      lg.mode_g, 
      lg.mode_ppg, 
      lg.mode_pim
)
SELECT 
  * 
FROM 
  overall_stats
ORDER BY 
  sog_avg DESC;

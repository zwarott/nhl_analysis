def basic_goalie_stats(num_games: Union[int, None] = None) -> pd.DataFrame:  
    """Scrape basic goalie stats for each team within games played.
    
    Parameters
    ----------
    num_games: Union[int, None] = None
        An integer, that represents number of games for which will be
        goalie stats scraped. If values is not specified, default value
        is None -> scrape all missing goalie stats.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame representing output dataset with basic 
        goalie stats. Each row corresponds to goalie's basic stats 
        for a single game.
    """
    # Last gid number in Game object
    game_gid = session.scalars(
        select(Game.gid)
        .order_by(Game.gid.desc())
        .limit(1)
    ).first()

    # Last gid number in GoalieStat object
    # Last gid number corresponds with index of next gid value
    goalie_stat_gid = session.scalars(
        select(GoalieStat.gid)
        .order_by(GoalieStat.gid.desc())
        .limit(1)
    ).first() 
   
    # If num_games is specified and GoalieStat object is not empty, modify
    # range of scraped goalie stats
    if num_games and goalie_stat_gid:
        game_gid = goalie_stat_gid + num_games

    # If num_games is sepcified and GoalieStat object is empty, modify range
    # of scraped goalie stats
    elif num_games and not goalie_stat_gid:
        game_gid = num_games
    
    # If TeamStat object is empty, set up value as O (first value from list)
    elif not goalie_stat_gid:
        goalie_stat_gid = 0

    # Get basic game info from database
    game_data = scraping_data()[goalie_stat_gid:game_gid]

    # Scrape game links for further scraping
    game_links = scraping_links()[goalie_stat_gid:game_gid]

    # Columns of output DataFrame
    new_columns = [
        'pid', 'tid', 'gid', 
        'dec', 'ga', 'sa', 
        'sv', 'svp', 'so', 
        'pim', 'en', 'enga', 
    ]

    # Scraped goalie stats
    scraped_basic_stats = [] 

    # Iterate over each game data and web link
    for idx, (data, link) in enumerate(zip(game_data, game_links), start=1):
        logger.info(f"Scraping {idx}/{len(game_data)} basic goalie stats...")

        gid, atid, htid = data[0], data[2], data[3]

        # Scrape skater stats for each game using pandas
        # 3 index -> atid | 5 index -> htid
        # Scrape goalie stats without total row (last row) and first 2 rows,
        # that represent headline rows
        # First level columns are removed as well
        atid_goalie_stats = pd.read_html(link)[3].iloc[:-1, 1:].droplevel(0, axis=1) 
        htid_goalie_stats = pd.read_html(link)[5].iloc[:-1, 1:].droplevel(0, axis=1)
        
        # Replace NaN values by 0
        atid_goalie_stats = atid_goalie_stats.where(pd.notnull(atid_goalie_stats), 0)
        htid_goalie_stats = htid_goalie_stats.where(pd.notnull(htid_goalie_stats), 0)

        # Player names in list for replacing them by pid values
        atid_goalies = atid_goalie_stats["Player"].tolist()
        htid_goalies = htid_goalie_stats["Player"].tolist()

        # Prepare pid values for away team
        atid_pid = []
        for goalie in atid_goalies:
            goalie_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == goalie)
            ).first()
            atid_pid.append(goalie_pid)
        
        # Prepare pid values for home team
        htid_pid = []
        for player in htid_goalies:
            player_pid = session.scalars(
                select(Player.pid)
                .where(Player.name == player)
            ).first()
            htid_pid.append(player_pid)
        
        # Relace player names by pid values
        atid_goalie_stats["Player"] = atid_pid        
        htid_goalie_stats["Player"] = htid_pid

        # Add tid column with values
        atid_goalie_stats.insert(1, "tid", atid) 
        htid_goalie_stats.insert(1, "tid", htid) 

        # Add gid column with values
        atid_goalie_stats.insert(2, "gid", gid) 
        htid_goalie_stats.insert(2, "gid", gid) 

        # Rename columns
        atid_goalie_stats.columns = new_columns
        htid_goalie_stats.columns = new_columns

        # Append team stats into list
        scraped_basic_stats.append(atid_goalie_stats)
        scraped_basic_stats.append(htid_goalie_stats)
        
        # Define sleep time to avoid error requests
        time.sleep(5)

    # Merge DataFrames into one DataFrame
    merged_df = pd.concat(scraped_basic_stats)

    return merged_df




import afltables_scraper as afl
import pandas as pd

# TODO: Clean random bits of text in DFS

def generate_afl_match_db(range):
  # Quick way but no user feedback
  # all_seasons = pd.concat([afl.create_df_for_season(str(year)) for year in range(1897, 1901)]) 

  # Slower way but user feedback
  dfs = []
  for year in range:
    print("Generating DF for season: " + str(year))
    dfs.append(afl.create_df_for_season(str(year)))

  all_seasons = pd.concat(dfs)

  all_seasons = all_seasons.reset_index().rename(columns={'index': 'yearly_match_number'})

  return all_seasons

# There were some cases of records being created as 'bye' rounds but were more informational text - see year 1900
# as the bye round was setting the team name in the 'home_team' column the 'away_team' column was pure and actually held 
# all the team names. By filtering the DF home_name column based on unique values of the away_team column we can 
# filter out these false positives. 
def remove_false_positive_bye_rounds(db):
  teams_in_league = db.away_team.unique()
  return db[db.home_team.isin(teams_in_league)]
  

if __name__ == "__main__":
  matches_db = generate_afl_match_db(range(1897,2021))

  print("Before cleaning length: " + str(len(matches_db.home_team)))
  
  clean_matches_db = remove_false_positive_bye_rounds(matches_db)

  print("After cleaning length: " + str(len(clean_matches_db.home_team)))

  print("Generate CSV File")
  clean_matches_db.to_csv("afltables_matches.csv")
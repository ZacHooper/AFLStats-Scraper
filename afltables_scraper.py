#TODO: Finals matches

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

## -- Various Functions to help sort through BS soups -- ##

# Only return tables that will contain the match tables. Ie": filter out all other table elements from the soup
def table_only_has_100_width(tag):
  return tag.name == "table" and tag.has_attr("width") and not tag.has_attr("style") and not tag.has_attr("border")

def get_finals_table(tag):
  return tag.name == "table" and tag.has_attr("style") and tag.has_attr("width") and tag.has_attr("border")

# Will return all the table elements containing the matches on a page
def get_round_tables(year):
  html_doc = requests.get("https://afltables.com/afl/seas/{0}.html".format(year))
  soup = BeautifulSoup(html_doc.content, 'lxml')
  rounds = soup.find_all(table_only_has_100_width)
  table_list = [table for table in rounds]
  return table_list

# Checks if the table element is a finals header
def check_if_finals_header(table_element):
  if table_element.b is None:
    return False
  if "Finals" in table_element.b.text:
    return False
  return "Final" in table_element.b.text

# Returns a list of Tuples containing the Finals Round Name and the table element of the match
def get_finals_round_tables(year):
  html_doc = requests.get("https://afltables.com/afl/seas/{0}.html".format(year))
  soup = BeautifulSoup(html_doc.content, 'lxml')
  final_matches = soup.find_all(get_finals_table)
  result = [table_element for table_element in final_matches if check_if_finals_header(table_element)]
  return [(finalsHeader.b.text ,finalsHeader.next_sibling.next_sibling) for finalsHeader in result]
  
  
# Return a list of a list of the table elements for each in match in the rounds provided
def get_match_tables(rounds):
  matches_td = [r.find_all("td", width="85%") for r in rounds]
  return [ value[0].find_all("table") for index, value in enumerate(matches_td) ]
  
# Format the given HTML of a match into a DataFrame
# Rename the columns to a more readable and useable value
def create_df_and_format(match):
  df = pd.read_html(str(match))
  match = df[0].rename(columns={0:"name", 1:"quarter_score", 2:"final_score", 3:"match_info"})
  return match

# The following functions return a specfic part of information found in the
# DF formatted by the 'create_df_and_format' function

# Return the team names in a JSON object with relevant keys
get_team_names = lambda match: {"home": match.name[0], "away": match.name[1]}

# --- Misc functions to get the quarter scores, format them or calculate the score of the quarter. ---
# Get quarter score returns all the quarter scores as a single string
get_quarter_scores = lambda match: {"home": match.quarter_score[0], "away": match.quarter_score[1]}

# Format quarter scores breaks up the string into a list
    # This is the breakdown of what each index will refer to
    # 0 = The full matched string - don't need to use this index,
    # 1 = Q1 Goals
    # 2 = Q1 Points
    # 3 = Q2 Goals
    # 4 = Q2 Points
    # 5 = Q3 Goals
    # 6 = Q3 Points
    # 7 = Q4 Goals
    # 8 = Q4 Points
format_quarter_scores = lambda score: re.match("\D*(\d*).(\d*)\D*(\d*).(\d*)\D*(\d*).(\d*)\D*(\d*).(\d*)", score)

# Given two integers (goals & points) return the score
calculate_quarter_score = lambda goals, points: (goals * 6) + points

# Return the final scores in a JSON object with relevant keys
get_final_scores = lambda match: {"home": match.final_score[0], "away": match.final_score[1]}

# Return a string of the day the match was held
get_match_day = lambda match: re.match("^(\w*)\s", match.match_info[0])[1]

# Return a string of the date the match was held
get_match_date = lambda match: re.match("^\w*\s(\d{2}-\w*-\d{4})", match.match_info[0])[1]

# Return a string of the time the match was held
get_match_time = lambda match: re.match("^\w*\s\d{2}-\w*-\d{4}\s(\d*:\d*\s\w*)", match.match_info[0])[1]

# Checks if the attendance is present in the table. Will return 'None' if it isn't
check_has_attendance = lambda match: re.match("^\w*\s\d{2}-\w*-\d{4}\s\d*:\d*\s\w*\sAtt:\s(\d*,\d*)", match.match_info[0])

# Return the attendance value as a string. If no attendance value present in the table return "N/A"
get_match_attendance = lambda match: check_has_attendance(match)[1] if check_has_attendance(match) is not None else "N/A"

# Return the venue value as a string
get_match_venue = lambda match: re.match(".*Venue:\s(.*)", match.match_info[0])[1]

# Returns a boolean for whether the match record (formatted by create_df_and_format) is a Bye match or not
# All bye match DataFrames will only contain 1 row. Thus if the length of the name column is '1' it must be a bye round
check_is_bye_match = lambda match_df: True if len(match_df.name) == 1 else False 

# Returns an object of all the relevant information to create a match record in the database
# Requires the Year of the match, the round the match was in & the a match df formatted by the create_df_and_format function
def create_match_record(year, rnd, match):
  match_day = get_match_day(match)
  match_date = get_match_date(match)
  match_time = get_match_time(match)
  match_attendance = get_match_attendance(match)
  match_venue = get_match_venue(match)
  team_names = get_team_names(match)
  quarter_scores = get_quarter_scores(match)
  home_quarter_scores = format_quarter_scores(quarter_scores['home'])
  away_quarter_scores = format_quarter_scores(quarter_scores['away'])
  final_scores = get_final_scores(match)

  return {
    "year": year,
    "round": rnd, 
    "day": match_day,
    "date": match_date,
    "time": match_time,
    "attendance": match_attendance,
    "venue": match_venue,
    "home_team": team_names['home'],
    "home_q1_goals": home_quarter_scores[1],
    "home_q1_points": home_quarter_scores[2],
    "home_q1_score": calculate_quarter_score(int(home_quarter_scores[1]), int(home_quarter_scores[2])),
    "home_q2_goals": home_quarter_scores[3],
    "home_q2_points": home_quarter_scores[4],
    "home_q2_score": calculate_quarter_score(int(home_quarter_scores[3]), int(home_quarter_scores[4])),
    "home_q3_goals": home_quarter_scores[5],
    "home_q3_points": home_quarter_scores[6],
    "home_q3_score": calculate_quarter_score(int(home_quarter_scores[5]), int(home_quarter_scores[6])),
    "home_q4_goals": home_quarter_scores[7],
    "home_q4_points": home_quarter_scores[8],
    "home_q4_score": calculate_quarter_score(int(home_quarter_scores[7]), int(home_quarter_scores[8])),
    "home_final_score": final_scores['home'],
    "away_team": team_names['away'],
    "away_q1_goals": away_quarter_scores[1],
    "away_q1_points": away_quarter_scores[2],
    "away_q1_score": calculate_quarter_score(int(away_quarter_scores[1]), int(away_quarter_scores[2])),
    "away_q2_goals": away_quarter_scores[3],
    "away_q2_points": away_quarter_scores[4],
    "away_q2_score": calculate_quarter_score(int(away_quarter_scores[3]), int(away_quarter_scores[4])),
    "away_q3_goals": away_quarter_scores[5],
    "away_q3_points": away_quarter_scores[6],
    "away_q3_score": calculate_quarter_score(int(away_quarter_scores[5]), int(away_quarter_scores[6])),
    "away_q4_goals": away_quarter_scores[7],
    "away_q4_points": away_quarter_scores[8],
    "away_q4_score": calculate_quarter_score(int(away_quarter_scores[7]), int(away_quarter_scores[8])),
    "away_final_score": final_scores['away'],
    "is_bye": 0
  }

# Returns an object of all the relevant information to create a 'BYE' match record in the database
# Requires the Year of the match, the round the match was in & the a match df formatted by the create_df_and_format function
create_bye_record = lambda year, rnd, match: {
                                                "year": year, 
                                                "round": rnd,
                                                "home_team": match.name[0],
                                                "is_bye": 1
                                              }

# Will determine which create method to use depending on whether the match is a Bye or not. 
# The match record must be formatted with create_df_and_format
create_record = lambda year, rnd, match: create_match_record(year, rnd, match) if not check_is_bye_match(match) else create_bye_record(year, rnd, match)

# Returns a DataFrame for all matches held in the provided season
def create_df_for_season(year):
  # Regular Season Matches
  rounds = get_round_tables(year)
  matches = get_match_tables(rounds)
  regular_season_matches = [create_record(year, roundNumber + 1, create_df_and_format(match)) for roundNumber, rnd in enumerate(matches) for match in rnd ]
  
  # Finals Matches - remember the tables function outputs a Tuple
  finals_tables = get_finals_round_tables(year)
  finals_matches = [create_record(year, match[0], create_df_and_format(match[1])) for match in finals_tables] 
  return pd.DataFrame(regular_season_matches + finals_matches)


if __name__ == "__main__":
  matches = create_df_for_season("1897")
  print(matches)

    
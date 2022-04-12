"""
This is a test script for handling source data which contains un-expected types of data when creating super cube

You can update super cube with different policies when adding table:
 - add      -> insert entirely new data
 - update   -> update existing data
 - upsert   -> simultaneously updates existing data and inserts new data
 - replace  -> truncates and replaces the data
"""

!pip install nba-api

from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players
import pandas as pd
from mstrio.connection import Connection
from mstrio.project_objects.datasets import SuperCube
first_player=0
player = players.get_players()
# fld="75028E138C4733B6C4DFE9964AE0F125"

base_url = "http://10.23.9.159:8080/MicroStrategyLibrary/api"
username = "administrator"
password = ""
connection = Connection(base_url, username, password, project_name="MicroStrategy Tutorial",
                        login_mode=1)
def playerstats(player_id):
    career = playercareerstats.PlayerCareerStats(player_id=player_id)
    return career.get_data_frames()[0]
player = players.get_players()[:20]
# print(len(player))
i = 0
for play_dict in player:
    if first_player==0:
        first_player=1
        career_df = playerstats(play_dict.get('id'))
    else:
       #career_df=career_df.concat(playerstats(player_id),keys=["PLAYER_ID"])
       career_df = pd.concat([career_df, playerstats(play_dict.get('id'))], ignore_index=True, sort=False)
   #    print(career_df)
    print(i)
    i += 1
# print(career_df)

# If the source data include non-numeric value like 'NaN' or 'None', but these columns ned to be considered as metrics, then it will have problem
# these columns will be considered as attributes, not metrics, so, we need to replaced these non-numeric values by '0'
career_df = career_df.where((pd.notnull(career_df)), 0)


# Even after replacing the special value, these columns data type will still be consdiered as object by pandas
# Convert column STL data type from Object to numeric 
career_df["STL"]=pd.to_numeric(career_df["STL"])
print(career_df.info())

print(career_df)

ds = SuperCube(connection=connection, name="NBA Players Stats - 01")
ds.add_table(name="PlayerStats", data_frame=career_df, to_attribute=["PLAYER_ID"], to_metric=None, update_policy="replace")
ds.create()


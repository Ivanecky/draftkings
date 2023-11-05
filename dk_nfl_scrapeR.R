# Code to get NFL odds from DraftKings
library(tidyverse)
library(httr)
library(dplyr)
library(RPostgreSQL)
library(DBI)
library(reshape2)
library(yaml)
library(rvest)
library(glue)

# Define base url
dk_nfl_url <- "https://sportsbook.draftkings.com/leagues/football/nfl"

# Grab links for events
dk_links <- dk_nfl_url %>%
  GET(., timeout(30)) %>%
  read_html() %>%
  html_nodes(xpath = "/html/body/div/div/section/section/section/div/div/div/div/div/div/div/div/div/table/tbody/tr/th/a") %>%
  html_attr("href") %>%
  unique()

# Navigate over event links to get data
# Define DK base
dk_base <- "https://sportsbook.draftkings.com"
test_link <- glue("{dk_base}{dk_links[2]}")

# Get html for game link
game_html <- test_link %>%
  GET(., timeout(30)) %>%
  read_html() 

# Get table titles
tbl_titles <- game_html %>%
  html_nodes("a.sportsbook-event-accordion__title") %>% 
  html_text()

# Get tables
game_tbls <- game_html %>%
  html_table()

# Drop first four titles
tbl_titles <- tbl_titles[5:length(tbl_titles)]

# Iterate over tables to extract 
# End at table 7 for player props
for(i in 1:7) {
  # Convert to dataframe
  df <- as.data.frame(game_tbls[i])
  
  # Check if first table
  if (i == 1) {
    # Overall game odds
    df <- df %>%
      rename(
        "team" = "Game",
        "spread" = "Spread",
        "total" = "Total",
        "moneyline" = "Moneyline"
      ) 
    
    # Assign home vs away. First row is the away team.
    df$home_v_away <- c("away", "home")
    
    # Pivot the data out
    df_wide <- pivot_wider(df, names_from = home_v_away, values_from = c(team, spread, total, moneyline))
    
    # Drop one total column and rename to just "total"
    df_wide <- df_wide %>%
      select(
        -c(total_home)
      ) %>%
      rename(
        "total" = "total_away"
      )
    
    # Split out odds for spread and for O/U total
    df_wide <- df_wide %>%
      mutate(
        # Team spreads
        spread_away_line = substr(spread_away, 1, nchar(spread_away) - 4),
        spread_away_odds = substr(spread_away, nchar(spread_away) - 3, nchar(spread_away)), 
        spread_home_line = substr(spread_home, 1, nchar(spread_home) - 4),
        spread_home_odds = substr(spread_home, nchar(spread_home) - 3, nchar(spread_home)),
        # Total
        total_pts = gsub("O", "", substr(total, 1, nchar(total) - 4)),
        total_odds = substr(total, nchar(total) - 3, nchar(total))
      ) %>%
      select(
        -c(spread_away, spread_home, total)
      ) 
      
    
    # Assign to game_odds
    if(exists("game_odds")) {
      game_odds <- rbind(game_odss, df_wide)
    } else {
      game_odds <- df_wide
    }
    
  } else {
    
    # Break out O/U odds
    df <- df %>%
      mutate(
        OVER_ODDS = substr(OVER, nchar(OVER) - 3, nchar(OVER)),
        OVER_LINE = substr(OVER, 1, nchar(OVER) - 4),
        UNDER_ODDS = substr(UNDER, nchar(UNDER) - 3, nchar(UNDER)),
      ) %>%
      select(
        -c(OVER, UNDER)
      ) %>%
      rename(
        "LINE" = "OVER_LINE"
      ) %>%
      mutate(
        LINE = stringr::str_trim(gsub("O", "", LINE), side = "both")
      )
    
    # Append player prop type via the table titles
    df$PROP_TYPE <- tbl_titles[i-1]
    
    # Append to overall prop table
    if(exists("plyr_props")) {
      plyr_props <- rbind(plyr_props, df)
    } else {
      plyr_props <- df
    }
  }
  
}


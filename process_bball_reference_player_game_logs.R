# Purpose: munge and process scraped NBA player game logs from Basketball Reference
rm(list = ls())
gc()
library(data.table)

# select season

season <- 2009
season <- paste0(season, '_', season + 1)

# set paths

in_path <- paste0('E:/NBA data collection/Data/Basketball Reference Player Game Logs/')
out_path <- 'E:/NBA data collection/Data/Processed Basketball Reference Player Game Logs/'

#########################################################################################################

# FUNCTIONS

# function to munge raw files and perform basic data quality checks

mungeFiles <- function(data_path, season_){
  # read in and append files to a single data table
  path_ <- paste0(data_path, 'season_', season_, '/')
  files <- list.files(path_)[grepl('csv', list.files(path_))]
  df_ <- data.table()
  for(file_ in files){
    print(file_)
    tmp <- fread(paste0(path_, file_))
    df_ <- rbind(df_, tmp)
  }
  # change CHO to CHA
  df_[TEAM == 'CHO', TEAM := 'CHA']
  df_[OPPONENT == 'CHO', OPPONENT := 'CHA']
  # perform basic data quality checks
  stopifnot(df_[, .(TotalMin = sum(MIN)), by = .(TEAM, GAMEDATE)][TotalMin < 235, .N] == 0)
  # change '-' in FG% fields to NA, then change columns to numeric
  stopifnot(df_[FGA > 0 & is.na(FGPERC), .N] == 0)
  stopifnot(df_[ATTEMPT2S > 0 & is.na(PERC2S), .N] == 0)
  stopifnot(df_[ATTEMPT3S > 0 & is.na(PERC3S), .N] == 0)
  stopifnot(df_[FTA > 0 & is.na(FTPERC), .N] == 0)
  # format GAMEDATE as date
  df_[, GAMEDATE2 := as.Date(GAMEDATE, format = '%m/%d/%Y')]
  df_[is.na(GAMEDATE2), GAMEDATE2 := as.Date(GAMEDATE, format = '%Y-%m-%d')]
  df_[, GAMEDATE := GAMEDATE2]
  df_[, GAMEDATE2 := NULL]
  stopifnot(df_[is.na(GAMEDATE), .N] == 0)
  # change HOME to be the team name
  df_[, HOME := as.character(HOME)]
  df_[HOME == 'TRUE', HOME := TEAM]
  df_[HOME == 'FALSE', HOME := OPPONENT]
  # add season
  df_[, Season := season_]
  # add GameID
  df_[TEAM == HOME, GameID := paste0(OPPONENT, '@', TEAM, ' ', GAMEDATE)]
  df_[OPPONENT == HOME, GameID := paste0(TEAM, '@', OPPONENT, ' ', GAMEDATE)]
  stopifnot(df_[is.na(GameID), .N] == 0)
  # add Winner
  df_[WL == 'W', Winner := TEAM]
  df_[WL == 'L', Winner := OPPONENT]
  # return full table
  return(df_)
}

# function to create Draftkings fantasy points per player game

DKpoints <- function(df_){
  df_[, tmp := apply(df_[, .(PTS, REB, AST, BLK, STL)], 1, function(x) sum(x > 9))]
  df_[, doubledouble := tmp >= 2]
  df_[, tripledouble := tmp > 2]
  df_[, DKpoints := PTS + .5 * MADE3S + 1.25 * REB + 1.5 * AST + 2 * STL + 2 * BLK - .5 * TO + 
        1.5 * doubledouble + 3 * tripledouble]
  df_[, tmp := NULL]
}

# function to collapse player logs and create team game logs

teamGameLogs <- function(df_){
  df_2 <- unique(df_[, .(GameID = unique(GameID), Season = unique(Season), OPPONENT = unique(OPPONENT), HOME = unique(HOME), 
                         WL = unique(WL), Winner = unique(Winner), DKpoints = sum(DKpoints), MIN = sum(MIN), 
                         PTS = sum(PTS), FGM = sum(FGM), FGA = sum(FGA), 
                         FGPERC = 100 * sum(FGM) / sum(FGA), MADE3S = sum(MADE3S), ATTEMPT3S = sum(ATTEMPT3S), 
                         PERC3S = 100 * sum(MADE3S) / sum(ATTEMPT3S), FTM = sum(FTM), FTA = sum(FTA), 
                         FTPERC = 100 * sum(FTM) / sum(FTA), OREB = sum(OREB), DREB = sum(DREB), REB = sum(REB), 
                         AST = sum(AST), STL = sum(STL), BLK = sum(BLK), TO = sum(TO), PF = sum(PF)), 
                     by = .(TEAM, GAMEDATE)])
  stopifnot(df_2[, .N, .(TEAM, GAMEDATE)][N > 1, .N] == 0)
  # number each game by team
  setkey(df_2, TEAM, GAMEDATE)
  df_2[, GameNum := 1:.N, by = TEAM]
  # create conceded stats
  concede <- copy(df_2)
  concede[, TEAM := OPPONENT]
  concede[, GameID := NULL]
  concede[, WL := NULL]
  concede[, Winner := NULL]
  concede[, OPPONENT := NULL]
  concede[, GameNum := NULL]
  concede[, HOME := NULL]
  concede[, Season := NULL]
  cols <- setdiff(names(concede), c('TEAM', 'GAMEDATE'))
  setnames(concede, cols, paste0('OPP_', cols))
  df_2 <- merge(df_2, concede, by = c('TEAM', 'GAMEDATE'))
  # return team logs
  return(df_2)
}

# function to create advanced team stats

AdvancedTeamStats <- function(teams_){
  # Possessions = FGA - (OREB)/(OREB + Opp DREB) * (FGA - FGM) * 1.07 + TO  + .4 * FTA
  teams_[, Possessions := .5 * ((FGA - (OREB)/(OREB + OPP_DREB) * (FGA - FGM) * 1.07 + TO  + .4 * FTA) + 
           (OPP_FGA - (OPP_OREB)/(OPP_OREB + DREB) * (OPP_FGA - OPP_FGM) * 1.07 + OPP_TO  + .4 * OPP_FTA))]
  # opponent possessions
  teams_[, Opp_Possessions := Possessions]
  # OffRtg = 100 * PTS / Possessions = opponent defensive rating
  teams_[, OffRtg := 100 * PTS / Possessions]
  teams_[, Opp_DefRtg := OffRtg]
  # DefRtg = 100 * OPP_PTS / Opp_Possessions = opponent offensive rating
  teams_[, DefRtg := 100 * OPP_PTS / Opp_Possessions]
  teams_[, Opp_OffRtg := DefRtg]
  # return team_logs
  return(teams_)
}

# function to create advanced player stats

AdvancedPlayerStats <- function(players_, teams_){
  # Usage = 100 * ((FGA + 0.44 * FTA + TO) * (TEAM MIN / 5)) / (MIN * (TEAM FGA + 0.44 * TEAM FTA + TEAM TO))
  setkey(players_, TEAM, GAMEDATE)
  setkey(teams_, TEAM, GAMEDATE)
  players_[teams_, `:=`(team_min = i.MIN, 
                        team_fga = i.FGA, 
                        team_fta = i.FTA, 
                        team_to = i.TO)]
  players_[, Usage := 100 * ((FGA + 0.44 * FTA + TO) * (team_min / 5)) / (MIN * (team_fga + 0.44 * team_fta + team_to))]
  players_[, `:=`(team_min = NULL, 
                  team_fga = NULL, 
                  team_fta = NULL, 
                  team_to = NULL)]
  stopifnot(players_[is.na(Usage) & MIN > 0, .N] == 0)
  
  # TrueShoot = 100 * (PTS / (2 * (FGA + (.44 * FTA))))
  players_[, TrueShoot := 100 * (PTS / (2 * (FGA + (.44 * FTA))))]
  
  # return player_logs
  return(players_)
}

#########################################################################################################

# munge files from season

players <- unique(mungeFiles(data_path = in_path, season_ = season))

# add DraftKings points

players <- DKpoints(df_ = players)

# create team game logs

teams <- teamGameLogs(df_ = players)

# add advanced team stats

teams <- AdvancedTeamStats(teams_ = teams)

# add advanced player stats

players <- AdvancedPlayerStats(players_ = players, teams_ = teams)

# save data

setkey(players, TEAM, GAMEDATE, PLAYERNAME)
setkey(teams, TEAM, GAMEDATE)

fwrite(players, paste0(out_path, 'processed_playerlogs_', season, '.csv'))
fwrite(teams, paste0(out_path, 'processed_teamlogs_', season, '.csv'))




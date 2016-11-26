# Load the libraries
library(jsonlite)
library(data.table)
library(dplyr)

# Initiate a file connection
fileCon <- gzfile('~/Dota/data/raw_data.gz', open="rb")


zz <- stream_in(fileCon, pagesize = 10000, progress = "=")

# Initiate all variables needed for the homemade stream function
NUM_MATCHES <- 5000
match.count <- 0
line.count <- 0

macro.data <- data.frame()
hero.data <- data.frame()

# Stream in using readLines until we reach the number of matches we want.
while(match.count < NUM_MATCHES) {
  next.line <- readLines(fileCon, n = 1)
  
  line.count <- line.count + 1
  
  # Only look at even numbered lines to avoid the [ and , lines.    
  if(line.count %% 2 == 0) {
    
    print(match.count)
    # read into JSON format
    readJSON <- jsonlite::fromJSON(next.line)
    
    # Up the match counter
    match.count <- match.count + 1
    
    # Load match macro data
    match.id <- readJSON$match_id
    game.length <- round(readJSON$duration/60, digits = 0)
    radiant.win <- readJSON$radiant_win
    macro.data <- rbind(macro.data, data.frame(match.id, game.length, radiant.win))
    
    # Cycle through each hero in the match, creating a list of kills, and other info.
    for(j in 1:10) {
      hero.id <- readJSON$players$hero_id[j]
      hero.radiant <- ifelse(j <= 5, 1, 0)
      kills <- readJSON$players$kills[j]
      deaths <- readJSON$players$deaths[j]
      assists <- readJSON$players$deaths[j]
      gpm <- readJSON$players$gold_per_min[j]
      xpm <- readJSON$players$xp_per_min[j]
      abandon <- ifelse(readJSON$players$leaver_status == 3, 1, 0)
      
      if ("npc_dota_roshan" %in% names(readJSON$players$killed)) {
        roshan.kills <- readJSON$players$killed$npc_dota_roshan
      } else {
        roshan.kills <- 0
      }
      #ancient.kills <- readJSON$ancientkills
      
      if ("CHAT_MESSAGE_FIRSTBLOOD" %in% readJSON$objectives$type) {
        first.blood <- ifelse(readJSON$objectives$player1[which(readJSON$objectives$type == "CHAT_MESSAGE_FIRSTBLOOD")] == (j-1), 1, 0)
      } else {
        first.blood <- 0
      }
      
      if ("multi_kills" %in% names(readJSON$players)) {
        if ("5" %in% names(readJSON$players$multi_kills)) {
          doublekills <- readJSON$players$multi_kills[j, 1]
          triplekills <- readJSON$players$multi_kills[j, 2]
          ultrakills <- readJSON$players$multi_kills[j, 3]
          rampages <- rowSums(data.frame(readJSON$players$multi_kills[j, 4:ncol(readJSON$players$multi_kills)]))
        } else if ("4" %in% names(readJSON$players$multi_kills)) {
          doublekills <- readJSON$players$multi_kills[j, 1]
          triplekills <- readJSON$players$multi_kills[j, 2]
          ultrakills <- readJSON$players$multi_kills[j, 3]
          rampages <- 0
        } else if ("3" %in% names(readJSON$players$multi_kills)) {
          doublekills <- readJSON$players$multi_kills[j, 1]
          triplekills <- readJSON$players$multi_kills[j, 2]
          ultrakills <- 0
          rampages <- 0
        } else if ("2" %in% names(readJSON$players$multi_kills)) {
          doublekills <- readJSON$players$multi_kills[j, 1]
          triplekills <- 0
          ultrakills <- 0
          rampages <- 0
        }
        else {
          doublekills <- 0
          triplekills <- 0
          ultrakills <- 0
          rampages <- 0
        }
      }
      if ("kill_streats" %in% names(readJSON$players)) {
        max.streak <- max(as.integer(colnames(readJSON$players$kill_streaks[j, which(!is.na(readJSON$players$kill_streaks[j, ]))])))
      } else {
        max.streak <- 0
      }
      
      if ("sen_log" %in% names(readJSON$players)) {
        sentries.placed <- length(readJSON$players$sen_log[[j]])
      } else {
        sentries.placed <- 0
      }
      
      if ("obs_log" %in% names(readJSON$players)) {
        obs.placed <- length(readJSON$players$obs_log[[j]])
      } else {
        obs.placed <- 0
      }
      
      hero.data <- rbind(hero.data, data.frame(match.id,
                                          hero.id,
                                          hero.radiant,
                                          kills,
                                          deaths,
                                          assists,
                                          gpm,
                                          xpm,
                                          #ancient.kills,
                                          roshan.kills,
                                          first.blood,
                                          doublekills,
                                          triplekills,
                                          ultrakills,
                                          rampages,
                                          max.streak,
                                          sentries.placed,
                                          obs.placed))
    }
  }
}

full.data <- left_join(hero.data, macro.data,
                       by = c("match.id" = "match.id"))
full.data$won <- ifelse(full.data$hero.radiant == 1, ifelse(full.data$radiant.win == T, 1, 0),
                        ifelse(full.data$radiant.win == F, 1, 0))
full.data <- select(full.data, -radiant.win, -hero.radiant)

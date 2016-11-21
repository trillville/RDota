# Load the libraries
library(jsonlite)
library(data.table)
library(plyr)

# Initiate a file connection
fileCon <- gzfile('~/Dota/data/raw_data.gz', open="rb")

# Initiate all variables needed for the homemade stream function
NUM_MATCHES <- 1000
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
      #ancient.kills <- readJSON$ancientkills
      #roshan.kills <- readJSON$playersnpc_dota_roshan
      if ("CHAT_MESSAGE_FIRSTBLOOD" %in% readJSON$objectives$type) {
        first.blood <- ifelse(readJSON$objectives$player1[which(readJSON$objectives$type == "CHAT_MESSAGE_FIRSTBLOOD")] == (j-1), 1, 0)
      }
      # if(exists(readJSON$players$multi_kills)) {
      #   
      # }
      # doublekills <- ifelse(readJSON$players$multi_kills$2
      # triplekills <- readJSON$triplekills
      # ultrakills <- readJSON$ultrakills
      # rampages <- readJSON$rampeges
      #best.streak <- getbestkillstreak
      sentries.placed <- length(readJSON$players$sen_log[[j]])
      obs.placed <- length(readJSON$players$obs_log[[j]])
      hero.data <- rbind(hero.data, data.frame(match.id,
                                          hero.id,
                                          hero.radiant,
                                          kills,
                                          deaths,
                                          assists,
                                          gpm,
                                          xpm,
                                          #ancient.kills,
                                          #roshan.kills,
                                          first.blood,
                                          #doublekills,
                                          #triplekills,
                                          #ultrakills,
                                          #rampages,
                                          #best.streak,
                                          sentries.placed,
                                          obs.placed))
    }
  }
}
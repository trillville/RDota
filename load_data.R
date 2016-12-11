# Load the libraries
library(jsonlite)
library(data.table)
library(dplyr)

# Initiate a file connection
fileCon <- gzfile('~/Dota/data/raw_data.gz', open="rb")

#z <- fromJSON(fileCon)

#zz <- stream_in(fileCon, pagesize = 10000, progress = "=")

# Initiate all variables needed for the homemade stream function
NUM_MATCHES <- 10000
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
    
    hero.id <- readJSON$players$hero_id
    match.id <- rep(match.id,length(hero.id))
    kills <- readJSON$players$kills
    hero.radiant <- ifelse(readJSON$players$player_slot > 10, 0, 1)
    deaths <- readJSON$players$deaths
    assists <- readJSON$players$deaths
    gpm <- readJSON$players$gold_per_min
    xpm <- readJSON$players$xp_per_min
    abandon <- ifelse(readJSON$players$leaver_status == 3, 1, 0)
    roshan.kills <- if (is.null(readJSON$players$killed$npc_dota_roshan)) rep(NA, 10) else readJSON$players$killed$npc_dota_roshan
    #first.blood <- ifelse(readJSON$objectives$player1[which(readJSON$objectives$type == "CHAT_MESSAGE_FIRSTBLOOD")] == (j-1), 1, 0)
    
    doublekills <- if (is.null(readJSON$players$multi_kills$`2`)) rep(NA, 10) else readJSON$players$multi_kills$`2`
    triplekills <- if (is.null(readJSON$players$multi_kills$`3`)) rep(NA, 10) else readJSON$players$multi_kills$`3`
    ultrakills <- if (is.null(readJSON$players$multi_kills$`4`)) rep(NA, 10) else readJSON$players$multi_kills$`4`
    rampages <- if (is.null(readJSON$players$multi_kills$`5`)) rep(NA, 10) else readJSON$players$multi_kills$`5`
    #max.streak <- max(as.integer(colnames(readJSON$players$kill_streaks[j, which(!is.na(readJSON$players$kill_streaks[j, ]))])))
    #sentries.placed <- length(readJSON$players$sen_log[[j]])
    #obs.placed <- length(readJSON$players$obs_log[[j]])
    
    item0 <- readJSON$players$item_0
    item1 <- readJSON$players$item_1
    item2 <- readJSON$players$item_2
    item3 <- readJSON$players$item_3
    item4 <- readJSON$players$item_4
    item5 <- readJSON$players$item_5
    
    hero.data <- rbind(hero.data,   data.frame(match.id,
                                    hero.id,
                                    hero.radiant,
                                    kills,
                                    deaths,
                                    assists,
                                    gpm,
                                    xpm,
                                    #ancient.kills,
                                    roshan.kills,
                                    #first.blood,
                                    doublekills,
                                    triplekills,
                                    ultrakills,
                                    rampages,
                                    #max.streak,
                                    #sentries.placed,
                                    #obs.placed,
                                    item0,
                                    item1,
                                    item2,
                                    item3,
                                    item4,
                                    item5))
  }
}

full.data <- left_join(hero.data, macro.data,
                       by = c("match.id" = "match.id"))
full.data$won <- ifelse(full.data$hero.radiant == 1, ifelse(full.data$radiant.win == T, 1, 0),
                        ifelse(full.data$radiant.win == F, 1, 0))
full.data <- select(full.data, -radiant.win, -hero.radiant)

# OHE encoding for all of the items
item.cols <- c("item0", "item1", "item2", "item3", "item4", "item5")
item.matrix <- as.matrix(full.data[, item.cols])
item.matrix <- item.matrix + 1 #start at 1 instead of zero

min.item <- min(item.matrix)
max.item <- max(item.matrix)
ohe.matrix <- matrix(nrow = nrow(full.data), ncol = (1 + max.item - min.item))

for (i in min.item:max.item) {
  ohe.matrix[, i] <- apply(item.matrix, 1, function(x) ifelse(i %in% x, 1, 0))
}
colnames(ohe.matrix) <- paste("item", as.character(seq(min.item, max.item)), sep = ".")

full.data <- cbind(full.data, ohe.matrix)

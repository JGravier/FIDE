library(tidyverse)
library(arrow)

pathcsv <- "Step 4 - Cleaning/Cleaned csvs/"
pathcsv <- list.files(path = pathcsv, pattern = "*.csv")

tableauranking <- read.csv(file = "Step 4 - Cleaning/Cleaned csvs/APR01.csv", sep = "*") %>%
  as_tibble()

tableauranking <- tableauranking %>%
  mutate(estpasvide = "1") %>%
  filter(estpasvide == 0) %>%
  select(-estpasvide) %>%
  select(ID_Number, Name, Title, Country, Rating, Games, Activity, Date) %>%
  mutate(ID_Number = as.character(ID_Number)) %>%
  mutate(Games = as.character(Games))

for (i in 1:length(pathcsv)) {
  
  tableauimport <- read.csv(file = paste0("Step 4 - Cleaning/Cleaned csvs/", pathcsv[i]), sep = "*") %>%
    as_tibble() %>%
    select(ID_Number, Name, Title, Country, Rating, Games, Activity, Date) %>%
    mutate(ID_Number = as.character(ID_Number)) %>%
    mutate(Games = as.character(Games)) %>%
    mutate(dateranking = str_remove_all(string = pathcsv[i], pattern = ".csv"))
  
  print(pathcsv[i])
  
  tableauranking <- tableauranking %>%
    bind_rows(tableauimport)
  
}

results <- tableauranking %>%
  group_by(dateranking) %>%
  arrange(desc(Rating), .by_group = TRUE) %>%
  mutate(ranking = rank(x = desc(Rating), ties.method = "min")) %>%
  ungroup()

write_parquet(x = results, sink = "FIDE_standard_compilations_Dahiya.parquet")


# Title: setup.R
# Author: Liam Tay Kearney
# Date: December 2022
# Version: 0.1
# Notes: setup for nyc-transit Shiny app
# Url: https://ltk2118.shinyapps.io/nyc-transit/

## LOAD DATA ------------------------------------
schools <- readRDS("rds/schools.rds")
geoms <- readRDS("rds/bklyn_geoms.rds")
acs <- readRDS("rds/bklyn_acs.rds")
trips <- readRDS("rds/trips_full.rds")
legs <- readRDS("rds/legs_full.rds") 
leg_colors <- readRDS("rds/leg_colors.rds")
agency_colors <- readRDS("rds/agency_colors.rds")
subway <- readRDS("rds/nyc-subway.rds")

## HELPER FUNCTIONS --------------------------
# function filter_trips 
# calculate accessible schools/slots from reactive school/trip data
filter_trips <- function(data = trips, cutoff = 90, select_run = "both", transfers = 5){
  
  if(select_run == "both"){
    
    counts <- data %>%
      filter(t_time <= cutoff) %>%
      filter(t_transit <= (transfers+1)) %>%
      group_by(geoid) %>%
        summarise(schools_accessible = n()/2,
                  slots_accessible = ceiling(sum(enrolment, na.rm = T)/2)) %>%
      ungroup() %>%
      right_join(geoms, by = "geoid") %>%
      mutate(schools_accessible = replace_na(schools_accessible, 0),
             slots_accessible = replace_na(slots_accessible, 0))
      
    }
  
  else{
      
      counts <- data %>%
        filter(t_time <= cutoff) %>%
        filter(t_transit <= (transfers+1)) %>%
        filter(run == select_run) %>%
        group_by(geoid) %>%
          summarise(schools_accessible = n(),
                    slots_accessible = sum(enrolment, na.rm = T)) %>%
        ungroup() %>%
        right_join(geoms, by = "geoid") %>%
        mutate(schools_accessible = replace_na(schools_accessible, 0),
               slots_accessible = replace_na(slots_accessible, 0))
    
  }
  
  return(counts)
  
}  



## CHOICES FOR UI INPUTS --------------------------

### schools ------------------
choices_size <- c("Under 1,000",
                  "1,000 - 4,999",
                  "5,000 - 9,999",   
                  "10,000 - 19,999",   
                  "20,000 and above")

choices_degree_level <- c("2-year",
                          "4-year or higher")

choices_degree_types <- c("Associate",
                      "Baccalaureate",
                      "Doctoral",
                      "Master's",
                      "Special Focus")

choices_sector <- c("Private for-profit",
                    "Private nonprofit",
                    "Public")

choices_carnegie <- schools %>% 
  select(size, sector, degree_level, degree_types, carnegie) %>% 
  unique()

### trip AM/PM ------------------------------ 

choices_run <- c("AM"="morning", "PM"="evening", "Both"="both")

### census data ------------------------------

choices_acs <- acs %>% select(label, concept) %>% unique()


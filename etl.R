# Title: etl.R
# Description: 
# Author: Liam Tay Kearney
# Date: Tue Dec 27 00:55:43 2022
# Version: 0.1
# Notes: 

# Load packages ------------------------------------------------
library(tidyverse)
library(lubridate)

# Set working directory ----------------------------------------
setwd(dirname(rstudioapi::getActiveDocumentContext()[["path"]]))

## Data loading
load_data <- function(dir = "./"){
  
  file_list <- list.files(dir,pattern = ".csv")
  
  for(file in file_list){
    
    # extract and store the file metadata in columns
    meta = str_match(file, "(.*)_(.*)_(.*)-(.*)\\.csv")
    
    file_data = read_csv(paste0(dir, file)) %>%
      rename(l_number = X1) %>%
      mutate(source_filename = meta[,1],
             source_run = meta[,2],
             source_apik = meta[,3],
             source_start_index = as.numeric(meta[,4]),
             source_end_index = as.numeric(meta[,5]),
             source_coverage = source_end_index - source_start_index)
    
    # if first file, create a data stack from file_data
    # else append file_data to the stack
    if(file == file_list[1]){
      data_stack <- file_data
    }
    else{
      data_stack <- data_stack %>%
        bind_rows(file_data)
    }
  }
  
  # validate end node successes, should be 126/126
  validation <- data_stack %>% 
    group_by(source_filename) %>%
    summarise(coverage = first(source_coverage)) %>%
    summarise(total_coverage = sum(coverage)) %>% 
    unlist() %>% 
    unname()
  
  message(paste("Total of", validation, "out of 126 destinations loaded."))
  if(validation == 126){message("Validation successful.")}
  else{message(paste("Validation unsuccessful.", 
                     126-validation, "missing nodes. Please try again."))}
  
  # record the percentage of o-d pairs for which the api threw an error
  n_origin = n_distinct(data_stack$origin)
  n_destination = n_distinct(data_stack$destination)
  n_pairs = nrow(unique(select(data_stack, origin, destination)))
  
  n_errors <- data_stack %>% 
    group_by(origin, destination) %>%
    summarise(errors = sum(duration=="-999")) %>%
    ungroup() %>%
    summarise(total_errors = sum(errors>0)) %>%
    unlist() %>% 
    unname()
  
  message(paste("Data loaded for ", n_pairs, "origin-destination pairs, with",
                n_origin, "origins and", 
                n_destination, "destinations."))
  
  message(paste(n_errors, "pairs with no data, or ",
                round(100*(n_errors/(n_pairs)),2),
                "percent."))
  
  # return the stack 
  return(data_stack)
  
}

## Data cleaning
clean <- function(data){
  
  data_clean <- data %>% 
    
    #remove "-999" and replace with NAs
    mutate(across(.cols = everything(),.fns = ~na_if(.x, "-999"))) %>%
    
    #create id primary key
    mutate(odid = if_else(source_run == "morning",
                          paste(as.character(origin), as.character(destination), sep = "-"),
                          paste(as.character(destination), as.character(origin), sep = "-"))) %>% 
    relocate(odid) %>%
    
    #create run (morning/evening) variable
    mutate(run = source_run) %>%
    relocate(run, .after = "odid") %>%
    
    #clean duration
    mutate(l_travelmins = str_extract(duration, "\\'value\\':\\s(\\d+)"),
           l_travelmins = as.numeric(str_extract(l_travelmins, "\\d+")),
           l_travelmins = round(l_travelmins/60,2)) %>%
    
    #clean distance
    mutate(l_distance = str_extract(distance, "\\'value\\':\\s(\\d+)"),
           l_distance = as.numeric(str_extract(l_distance, "\\d+"))) %>%
    
    #clean leg number
    mutate(l_number = l_number + 1) %>% #convert from pythonic index 
    
    #clean lat/lons - extract from location information
    mutate(l_lat_arr = as.double(str_match(end_location, 
                                           "\\'lat\\':\\s(\\d+\\.\\d+)")[,2]),
           l_lon_arr = as.double(str_match(end_location, 
                                           "\\'lng\\':\\s(-*\\d+\\.\\d+)")[,2]),
           l_lat_dep = as.double(str_match(start_location, 
                                           "\\'lat\\':\\s(\\d+\\.\\d+)")[,2]),
           l_lon_dep = as.double(str_match(start_location, 
                                           "\\'lng\\':\\s(-*\\d+\\.\\d+)")[,2])) %>%
    
    # adjust column orderings
    relocate(c(origin, destination, l_number, 
               l_lat_dep, l_lon_dep, l_lat_arr, l_lon_arr,
               l_travelmins, l_distance), 
             .after = 1) %>%
    
    #clean and parse information from transit_details (a json-like field)
    mutate(l_dep_stop = str_match(transit_details, 
                                  "departure_stop(.*?)\\'name\\':\\s\\'(.*?)\\'")[,3],
           l_dep_time = str_replace(str_match(transit_details, 
                                              "departure_time(.*?)\\'text\\':\\s\\'(.*?)\\'")[,3],
                                    "\\\\u202f", " "),
           l_instructions = html_instructions,
           l_arr_stop = str_match(transit_details, 
                                  "arrival_stop(.*?)\\'name\\':\\s\\'(.*?)\\'")[,3],
           l_arr_time = str_replace(str_match(transit_details, 
                                              "arrival_time(.*?)\\'text\\':\\s\\'(.*?)\\'")[,3],
                                    "\\\\u202f", " ")) %>%
    rename(l_mode = travel_mode) %>%
    mutate(l_agency = str_match(transit_details, 
                                "agencies(.*?)\\'name\\':\\s\\'(.*?)\\'")[,3],
           l_agency = replace(l_agency, l_agency == "MTA New York City Transit", "MTA - NYC Subway"),
           l_agency = replace(l_agency, l_agency == "MTA", "MTA - NYC Bus"),
           l_agency = replace(l_agency, l_agency == "NJ TRANSIT BUS", "NJ Transit Bus"),
           l_line = str_match(transit_details, 
                              "agencies(.*?)\\s\\'name\\':\\s\\'(.*?)\\'")[,3],
           l_line = str_replace(l_line, ", NYC Subway", ""),
           l_color = str_match(transit_details, 
                               "agencies(.*?)\\'color\\':\\s\\'(.*?)\\'")[,3],
           l_text_color = str_match(transit_details, 
                                    "agencies(.*?)\\'text_color\\':\\s\\'(.*?)\\'")[,3],
           l_vehicle = str_match(transit_details, 
                                 "vehicle(.*?)\\'type\\':\\s\\'(.*?)\\'")[,3],
           l_num_stops = as.integer(str_match(transit_details, 
                                              "vehicle(.*?)\\'num_stops\\':\\s(\\d+)")[,3])) %>%
    
    #parse and standardize times - convert to POSIXct
    mutate(source_t_set = if_else(source_run == "morning", 
                                  as.character(arrival_time), 
                                  as.character(depart_time))) %>% 
    mutate(source_t_set_date = str_match(source_t_set, "(.*?)\\s(.*?)")[,2]) %>% 
    mutate(t_arrival_actual = parse_date_time(paste(source_t_set_date, actual_arrival), '%m/%d/%Y %I:%M %p'),
           t_departure_actual = parse_date_time(paste(source_t_set_date, actual_departure), '%m/%d/%Y %I:%M %p')) %>% 
    mutate(t_set = if_else(source_run == "morning",
                           parse_date_time(paste(source_t_set_date,"9:00 AM"), '%m/%d/%Y %I:%M %p'),
                           parse_date_time(paste(source_t_set_date,"5:00 PM"), '%m/%d/%Y %I:%M %p'))) %>% 
    mutate(l_dep_time = parse_date_time(paste(source_t_set_date, l_dep_time), '%m/%d/%Y %I:%M %p', quiet = T),
           l_arr_time = parse_date_time(paste(source_t_set_date, l_arr_time), '%m/%d/%Y %I:%M %p', quiet = T)) %>% 
    
    #calculate time intervals
    group_by(odid, origin, destination) %>%
    mutate(t_travelmins = round(sum(l_travelmins, na.rm = T),0)) %>%
    ungroup() %>% 
    mutate(t_set_actual = if_else(source_run == "morning",
                                  difftime(t_set, t_departure_actual, units = "mins"),
                                  difftime(t_arrival_actual, t_set, units = "mins")),
           t_actual_actual = difftime(t_arrival_actual, t_departure_actual, units = "mins"),
           t_travelmins = make_difftime(minute = t_travelmins, units = "mins"),
           t_waitmins = t_set_actual - t_travelmins,
           t_waitactual = t_actual_actual - t_travelmins) %>%
    
    # adjust column orderings (again)
    relocate(c(t_set, t_departure_actual, t_arrival_actual,
               t_set_actual, t_actual_actual, t_travelmins, t_waitmins),
             .after = "l_num_stops") %>%
    
    #remove unwanted columns, rename and reorder columns
    select(
      -steps, 
      -actual_departure,
      -actual_arrival,
      -html_instructions, 
      -transit_details, 
      -transit_routing_preference,
      -total_time,
      -depart_time,
      -arrival_time,
      -end_location, 
      -start_location, 
      -distance, 
      -duration
    ) %>%
    
    rename(source_request_time = request_time) %>% 
    relocate(starts_with("source"),.after = last_col()) %>%
    
    select(-l_instructions, -l_arr_time, -l_dep_time,
           -starts_with("l_lat"), -starts_with("l_lon"),
           -t_set_actual, -t_travelmins, -t_waitmins,
           -t_set) %>%
    rename(t_departure = t_departure_actual,
           t_arrival = t_arrival_actual,
           t_time = t_actual_actual,
           t_waiting = t_waitactual) %>%
    
    ##FILTERING OCCURS HERE FOR <100 MIN TRIPS
    filter(t_time < 100) %>%
    
    #CALCULATE NUMBER OF TRANSFERS
    group_by(odid, run) %>%
    mutate(t_transfer = sum(l_mode == 'TRANSIT')-1) %>%
    ungroup()
  
  return(data_clean)
}

## Data flattening
flatten <- function(data){
  
  data_flattened <- data %>% 
    
    group_by(odid) %>%
    summarise(t_nlegs = max(l_number, na.rm = T),
              t_walking = sum(l_mode == "WALKING", na.rm = T),
              t_transit = sum(l_mode == "TRANSIT", na.rm = T),
              t_bus = sum(l_vehicle == "BUS", na.rm = T),
              t_ferry = sum(l_vehicle == "FERRY", na.rm = T),
              t_heavyrail = sum(l_vehicle == "HEAVY_RAIL", na.rm = T),
              t_subway = sum(l_vehicle == "SUBWAY", na.rm = T),
              t_tram = sum(l_vehicle == "TRAM", na.rm = T)) 
  
    data_flattened <- data_flattened %>%
      left_join(unique(select(data, -starts_with("l_")))) %>%
      mutate(school_id = if_else(run == "morning",
                                 destination,
                                 origin),
             geoid = if_else(run == "morning",
                                origin,
                                destination)) %>%
      relocate(c(origin, destination, school_id, geoid, run), .after = "odid")
      
  
    return(data_flattened)
}

## Preprocessing schools dataset
process_schools <- function(ipeds_data){
  
  schools <- ipeds_data %>%
    select(-EFFYALEV, -EFFYLEV, -LSTUDY, 
           -own_THEOLOG, -own_OPEFLAG, -own_FORPROFIT,
           -LON_CTR, -LAT_CTR, -crow_flies,
           -own_ADMCON7, -starts_with('ADMSSN'),
           -starts_with('APPLCN')) %>%
  mutate(ADDR = paste(ADDR, CITY, ZIP, sep = ", ")) %>%
  rename(Enrolment = EFYTOTLT) %>%
  mutate('Percent male' = EFYTOTLM/Enrolment,
          'Percent female' = EFYTOTLW/Enrolment,
          'Percent white' = EFYWHITT/Enrolment,
          'Percent Black' = EFYBKAAW/Enrolment,
          'Percent Hispanic' = EFYHISPT/Enrolment,
          'Percent AAPI' = (EFYASIAT+EFYNHPIT)/Enrolment,
         across(`Percent male`:`Percent AAPI`, ~(100*round(.x, 3)))) %>%
    select(-starts_with('EFY'))
  
  colnames(schools) <- str_to_lower(colnames(schools))
  colnames(schools) <- str_remove(colnames(schools), "own_")
  
  return(schools)
}

filter_time <- function(time){
  
  blockgroups <- trips_full %>%
    filter(t_time < time) %>%
    group_by(geoid, run) %>%
      summarise(n = n(),
                slots = sum(enrolment, na.rm = T)) %>%
    ungroup() %>%
    right_join(all_blockgroup_ids, by = c("geoid", "run")) %>%
    mutate(across(n:slots, .fns = ~replace_na(.x, 0)))
  
  avgs <- blockgroups %>%
    group_by(geoid) %>%
    summarise(run = "average",
              n = mean(n, na.rm = T),
              slots = ceiling(mean(slots,na.rm = T)))
  
  return(bind_rows(blockgroups, avgs) %>%
    mutate(cutoff = time))
  
}  

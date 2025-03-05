# Kameron Lloyd
# 02/10/2025
# #####
# Description: We received data from jurisdictions on dates lots were cleared of 
# debris, issued permits, and issued certificates of occupancy after 5 wildfires.
# This code identifies the start and end dates of the three phases: debris clearance,
# permit issuance, and COO issuance, after different wildfires and creates monthly
# cumulative stats of the number of properties cleared each month after the date
# of disaster
# #####

####----LIBRARIES----####
library(tidyverse)
library(dplyr)
library(here)
library(lubridate)

# ------------------------------MARSHALL FIRE-------------------------------------

####----BOULDER COUNTY----####
boulder_raw <- read.csv(here("data-raw", "marshall", "boulder", "boulder_raw.csv")) 

# Converting to date format
boulder <- boulder_raw %>%
  mutate(
    across(c(Cleared, Permit, CO, Sold, Disaster.Date), mdy)
  ) 

# Creating the monthly intervals for 6.25 years (this goes past the current date
# so that it is still the same length as the other fires)
time_intervals <- seq(1, 75, by = 1)

# Counting by month after disaster
boulder_m <- boulder %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = Disaster.Date %m+% months(interval))%>%
  group_by(interval, end_date)%>%
  summarise(
    num_cleared = sum(Cleared <= end_date, na.rm = TRUE),
    num_permit_issued = sum(Permit <= end_date, na.rm = TRUE),
    num_coo_issued = sum(CO <= end_date, na.rm = TRUE)
  ) %>%
  ungroup()

# Finding the start and end dates of each phase
boulder_start_end <- boulder %>%
  summarise(
    across(c(Cleared, Permit, CO, Sold), list(min = ~min(.x, na.rm = TRUE), 
                                              max = ~max(.x, na.rm = TRUE)))
  )

####----LOUISVILLE----####
# Converting to date format for the debris clearance data
louisville_demo_raw <- read.csv(here("data-raw", "marshall", "louisville", "louisville_demo_raw.csv"))%>%
  mutate(
    debris_cleared = mdy(Finalize.Date))

# Finding the start and end dates of debris clearance
louisville_debris_start_end <- louisville_demo_raw %>%
  summarise(
    across(debris_cleared, list(min = ~min(.x, na.rm = TRUE), 
                                                     max = ~max(.x, na.rm = TRUE)))
)

# Converting to date format for permit/COO data and removing the temporary permits
# that have "TEMP" in their permit number
louisville_pcoo_raw <- read.csv(here("data-raw", "marshall", "louisville", "louisville_pcoo_raw.csv"))%>%
  mutate(
    across(c(Permit.Issue.Date, Permit.Finaled.Date), mdy)
  )%>%
  filter(!grepl("TEMP", Permit.Number, ignore.case = TRUE))

# Finding the start and end dates of permit/COO issuance
louisville_pcoo_start_end <- louisville_pcoo_raw %>%
  summarise(
    across(c(Permit.Issue.Date, Permit.Finaled.Date), list(min = ~min(.x, na.rm = TRUE), 
                                max = ~max(.x, na.rm = TRUE)))
  )

# Combining start and end dates into one dataframe
louisville_start_end <- cbind(louisville_debris_start_end, louisville_pcoo_start_end)

# Now figuring out the number of lots cleared, permits issued, and COOs issued per month
louisville_demo_m <-louisville_demo_raw %>%
  select(debris_cleared) 

louisville_pcoo_m <- louisville_pcoo_raw %>%
  select(Permit.Issue.Date, Permit.Finaled.Date)

# Make the datasets the same length to bind
max_rows <- max(nrow(louisville_demo_m), nrow(louisville_pcoo_m))

# Expand both datasets to match the max row count
louisville_demo_m <- louisville_demo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

louisville_pcoo_m <- louisville_pcoo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

# Bind them side by side
louisville_combined <- bind_cols(louisville_demo_m, louisville_pcoo_m)%>%
  mutate(disaster_date = as.Date("2022-01-01"))

# Creating the time intervals
time_intervals <- seq(1, 75, by = 1)

# Counting by month after disaster
louisville_m <- louisville_combined %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = disaster_date %m+% months(interval))%>%
  group_by(interval, end_date)%>%
  summarise(
    num_cleared = sum(debris_cleared <= end_date, na.rm = TRUE),
    num_permit_issued = sum(Permit.Issue.Date <= end_date, na.rm = TRUE),
    num_coo_issued = sum(Permit.Finaled.Date <= end_date, na.rm = TRUE)
  ) %>%
  ungroup()


####----SUPERIOR----####
# Converting to date format for the debris clearance data
superior_demo_raw <- read.csv(here("data-raw", "marshall", "superior", "superior_demo_raw.csv"))%>%
  mutate(
    debris_cleared = mdy(Certificate.of.Completion.IssuedDate))

# Finding the start and end dates of debris clearance
superior_debris_start_end <- superior_demo_raw %>%
  summarise(
    across(debris_cleared, list(min = ~min(.x, na.rm = TRUE), 
                                max = ~max(.x, na.rm = TRUE)))
  )

# Converting to date format for the permit/COO data
superior_pcoo_raw <- read.csv(here("data-raw", "marshall", "superior", "superior_pcoo_raw.csv"))%>%
    mutate(
      across(c(Acceptance.Date, Certificate.of.Occupancy.Issued.Date), mdy)
    ) 
    
# Finding the start and end dates of permit/COO issuance
superior_pcoo_start_end <- superior_pcoo_raw %>%
  summarise(
    across(c(Acceptance.Date, Certificate.of.Occupancy.Issued.Date), list(min = ~min(.x, na.rm = TRUE), 
                                                           max = ~max(.x, na.rm = TRUE)))
  )

# Combining start and end dates into one data frame
superior_start_end <- cbind(superior_debris_start_end, superior_pcoo_start_end)

# Creating the monthly analysis
superior_demo_m <-superior_demo_raw %>%
  select(debris_cleared) 

superior_pcoo_m <- superior_pcoo_raw %>%
  select(Acceptance.Date, Certificate.of.Occupancy.Issued.Date)

# Make the datasets the same length to bind
max_rows <- max(nrow(superior_demo_m), nrow(superior_pcoo_m))

# Expand both datasets to match the max row count
superior_demo_m <- superior_demo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

superior_pcoo_m <- superior_pcoo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

# Bind them side by side
superior_combined <- bind_cols(superior_demo_m, superior_pcoo_m)%>%
  mutate(disaster_date = as.Date("2022-01-01"))

# Creating the time intervals
time_intervals <- seq(1, 75, by = 1)

# Counting by month after disaster
superior_m <- superior_combined %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = disaster_date %m+% months(interval))%>%
  group_by(interval, end_date)%>%
  summarise(
    num_cleared = sum(debris_cleared <= end_date, na.rm = TRUE),
    num_permit_issued = sum(Acceptance.Date <= end_date, na.rm = TRUE),
    num_coo_issued = sum(Certificate.of.Occupancy.Issued.Date <= end_date, na.rm = TRUE)
  ) %>%
  ungroup()

# Combining to fire level

marshall_m <- bind_rows(superior_m, louisville_m, boulder_m) %>%
  group_by(interval, end_date) %>%
  summarise(across(c(num_cleared, num_permit_issued, num_coo_issued), sum, na.rm = TRUE), .groups = "drop")

write.csv(marshall_m, "marshall_monthly.csv")

# ------------------------------MAUI FIRE-------------------------------------
####----MAUI COUNTY----####
# Converting to date format for the debris clearance data
maui_demo_raw <- read.csv(here("data-raw", "maui", "maui", "maui_demo_raw.csv"))%>%
  mutate(
    debris_cleared = mdy(Permit.Finaled.Date))


# Finding the start and end dates of debris clearance
maui_debris_start_end <- maui_demo_raw %>%
  summarise(
    across(debris_cleared, list(min = ~min(.x, na.rm = TRUE), 
                                max = ~max(.x, na.rm = TRUE)))
  )

# Converting to date format for the permit/COO data
maui_permit_raw <- read.csv(here("data-raw", "maui", "maui", "maui_pcoo_raw.csv"))%>%
  mutate(
   permit_issued = mdy(Permit.Issue.Date))

# Finding the start and end dates of permit/COO issuance
maui_permit_start_end <- maui_permit_raw %>%
  summarise(
    across(permit_issued, list(min = ~min(.x, na.rm = TRUE), 
                                                                          max = ~max(.x, na.rm = TRUE)))
  )

# Combining start and end dates into one data frame
maui_start_end <- cbind(maui_debris_start_end, maui_permit_start_end)

# Creating the monthly analysis
maui_demo_m <-maui_demo_raw %>%
  select(debris_cleared) %>%
  mutate(disaster_date = as.Date("2023-08-08"))

maui_permit_m <- maui_permit_raw %>%
  select(permit_issued)

# Make the datasets the same length to bind
max_rows <- max(nrow(maui_demo_m), nrow(maui_permit_m))

# Expand both datasets to match the max row count
maui_demo_m <- maui_demo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

maui_permit_m <- maui_permit_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

# Bind them side by side
maui_combined <- bind_cols(maui_demo_m, maui_permit_m)

# Creating the time intervals
time_intervals <- seq(1, 75, by = 1)

# Counting by month after disaster
maui_m <- maui_combined %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = disaster_date %m+% months(interval))%>%
  group_by(interval, end_date)%>%
  summarise(
    num_cleared = sum(debris_cleared <= end_date, na.rm = TRUE),
    num_permit_issued = sum(permit_issued <= end_date, na.rm = TRUE),
  ) %>%
  ungroup()

write.csv(maui_m, "maui_monthly.csv")

# ------------------------------CAMP FIRE-------------------------------------
####----BUTTE COUNTY----####
# Converting to date format for the permit/COO data
butte_pcoo_raw <- read.csv(here("data-raw", "camp", "butte", "butte_pcoo_raw.csv"))%>%
  filter(INCIDENT_NAME == "Camp Fire")%>%
  mutate(
    across(c(ISSUED, FINALED), mdy)
  ) 

# Finding the start and end dates of permit/COO issuance
butte_pcoo_start_end <- butte_pcoo_raw %>%
  summarise(
    across(c(ISSUED, FINALED), list(min = ~min(.x, na.rm = TRUE),
                                    max = ~max(.x, na.rm = TRUE)))
  )

# Creating the quarterly analysis
butte_pcoo_m <-butte_pcoo_raw %>%
  select(ISSUED, FINALED) %>%
  mutate(disaster_date = as.Date("2018-11-08"))

# Creating the time intervals
time_intervals <- seq(1, 75, by = 1)

# Counting by month after disaster
butte_m <- butte_pcoo_m %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = disaster_date %m+% months(interval))%>%
  group_by(interval, end_date)%>%
  summarise(
    num_permit_issued = sum(ISSUED <= end_date, na.rm = TRUE),
    num_coo_issued = sum(FINALED <= end_date, na.rm = TRUE)
  ) %>%
  ungroup()

####----PARADISE----####

# Debris start and end is Feb 2019 and Nov 2019

# Converting to date format for the permit/COO data
paradise_pcoo_raw <- read.csv(here("data-raw", "camp", "paradise", "paradise_pcoo_raw.csv"))%>%
  mutate(
    across(c(Date.issued, date.finaled), mdy)
  ) 

# Finding the start and end dates of permit/COO issuance
paradise_pcoo_start_end <- paradise_pcoo_raw %>%
  summarise(
    across(c(Date.issued, date.finaled), list(min = ~min(.x, na.rm = TRUE),
                                    max = ~max(.x, na.rm = TRUE)))
  )

# Creating the monthly analysis
paradise_pcoo_m <-paradise_pcoo_raw %>%
  select(Date.issued, date.finaled) %>%
  mutate(disaster_date = as.Date("2018-11-08"))

# Creating the time intervals
time_intervals <- seq(1, 75, by = 1)

# Counting by month after disaster
paradise_m <- paradise_pcoo_m %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = disaster_date %m+% months(interval))%>%
  group_by(interval, end_date)%>%
  summarise(
    num_permit_issued = sum(Date.issued <= end_date, na.rm = TRUE),
    num_coo_issued = sum(date.finaled <= end_date, na.rm = TRUE)
  ) %>%
  ungroup()

# Combining the jurisdictions to create a fire level table
camp_m <- bind_rows(paradise_m, butte_m) %>%
  group_by(interval, end_date) %>%
  summarise(across(c(num_permit_issued, num_coo_issued), sum, na.rm = TRUE), .groups = "drop")

write.csv(camp_m, "camp_monthly.csv")

# ------------------------------CARR FIRE-------------------------------------
####----SHASTA COUNTY----####


# Converting to date format for the permit/COO data
shasta_pcoo_raw <- read.csv(here("data-raw", "carr", "shasta", "shasta_pcoo_raw.csv"))%>%
  mutate(
    across(c(ISSUED, FINALED), mdy)
  ) %>%
  filter(PERMITSUBTYPE != "FIRE REPAIR")%>%
  filter(PERMITSUBTYPE != "RETAINING WALL")


# Finding the start and end dates of permit/COO issuance
shasta_pcoo_start_end <- shasta_pcoo_raw %>%
  summarise(
    across(c(ISSUED, FINALED), list(min = ~min(.x, na.rm = TRUE),
                                              max = ~max(.x, na.rm = TRUE)))
  )
# This dataset is lots cleared by the state program with all jurisdictions data (including Redding) 
# so filtering to shasta county using the address
shasta_pub_demo_state <- read.csv(here("data-raw", "carr", "shasta", "shasta_public_demo_state.csv")) %>%
  mutate(debris_cleared = mdy(FINAL.INSPECT),
         APN = gsub("-", "", APN)) %>%
  mutate(APN = paste0(APN, "000")) %>%
  select(debris_cleared, APN)
  
# # This dataset contains addresses cleared by the EHD  - the APN values 
# # are distinct between these two datasets so should be no overlapping lots
# shasta_pub_demo_EHD <- read.csv(here("data-raw", "carr", "shasta", "shasta_public_demo_ehd.csv"))%>%
#   filter(grepl('Shasta|SHASTA', ADR)) %>%
#   mutate(across(c(Cal_Recycle), mdy),
#          APN = gsub("-", "", ASMT)
#   ) %>%
#   rename(debris_cleared_ehd = Cal_Recycle)%>%
#   select(debris_cleared_ehd, APN)

# shasta_pub_demo <- full_join(shasta_pub_demo_EHD, shasta_pub_demo_state, by = "APN")

  
# This dataset was maintained by Shasta County and includes all lots cleared privately
# via demolition permits
shasta_priv_demo <- read.csv(here("data-raw", "carr", "shasta", "shasta_private_demo.csv"))%>%
  filter(PERMITSUBTYPE != "DEMO COMMERCIAL BUILDING") %>%
  filter(PERMITSUBTYPE != "DEMO POOL/SPA") %>%
  mutate(debris_cleared = mdy(FINALED)) %>%
  mutate(APN = str_pad(SITE_APN, width = 12, pad = "0", side = "left"))%>%
  select(debris_cleared, APN)

shasta_demo_raw <- bind_rows(shasta_priv_demo, shasta_pub_demo_state)

# Finding the start and end dates of debris clearance
shasta_demo_start_end <- shasta_demo_raw %>%
  summarise(
    across(debris_cleared, list(min = ~min(.x, na.rm = TRUE),
                                    max = ~max(.x, na.rm = TRUE)))
  )

shasta_start_end <- cbind(shasta_demo_start_end, shasta_pcoo_start_end)


# Monthly analysis
shasta_demo_m <-shasta_demo_raw %>%
  select(debris_cleared) 

shasta_pcoo_m <- shasta_pcoo_raw %>%
  select(ISSUED, FINALED)

# Make the datasets the same length to bind
max_rows <- max(nrow(shasta_demo_m), nrow(shasta_pcoo_m))

# Expand both datasets to match the max row count
shasta_demo_m <- shasta_demo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

shasta_pcoo_m <- shasta_pcoo_m %>%
  mutate(row_id = row_number()) %>%
  complete(row_id = 1:max_rows) %>%
  select(-row_id)

# Bind them side by side
shasta_combined <- bind_cols(shasta_demo_m, shasta_pcoo_m)%>%
  mutate(disaster_date = as.Date("2018-07-23"))

# Creating the time intervals
time_intervals <- seq(1, 79, by = 1)

# Counting by month after disaster
shasta_m <- shasta_combined %>%
  mutate(ID = row_number()) %>%
  crossing(interval = time_intervals) %>%
  mutate(end_date = disaster_date %m+% months(interval))%>%
  group_by(interval,end_date)%>%
  summarise(
    num_cleared = sum(debris_cleared <= end_date, na.rm = TRUE),
    num_permit_issued = sum(ISSUED <= end_date, na.rm = TRUE),
    num_coo_issued = sum(FINALED <= end_date, na.rm = TRUE)
  ) %>%
  ungroup()

write_csv(shasta_m, "shasta_quarterly.csv")

# ------------------------------TUBBS FIRE-------------------------------------
####----SANTA ROSA----####
# Converting to date format for the permit/COO data
# sr_pcoo_raw <- read.csv(here("data-raw", "tubbs", "santa rosa", "santa_rosa_pcoo_raw.csv"))%>%
#   mutate(
#     across(c(HasIssuedDt_pPermit, HasFinalDt_pPermit), mdy_hms)
#   ) 
# 
# # Finding the start and end dates of permit/COO issuance
# sr_pcoo_start_end <- sr_pcoo_raw %>%
#   summarise(
#     across(c(HasIssuedDt_pPermit, HasFinalDt_pPermit), list(min = ~min(.x, na.rm = TRUE),
#                                     max = ~max(.x, na.rm = TRUE)))
#   )
# 
# # Converting to date format for the debris data (only private)
# sr_demo_raw <- read.csv(here("data-raw", "tubbs", "santa rosa", "santa_rosa_demo_raw.csv"))%>%
#   mutate(
#    debris_clearance = mdy(PermitFinalDate)
#   ) 
# 
# # Finding the start and end dates of debris clearance (again only private)
# sr_demo_start_end <- sr_demo_raw %>%
#   summarise(
#     across(PermitFinalDate, list(min = ~min(.x, na.rm = TRUE),
#                                                             max = ~max(.x, na.rm = TRUE)))
#   )
# 
# sr_start_end <- cbind(sr_demo_start_end, sr_pcoo_start_end)
# 
# # Quarterly analysis
# sr_demo_q <-sr_demo_raw %>%
#   select(debris_clearance) 
# 
# sr_pcoo_q <- sr_pcoo_raw %>%
#   select(HasIssuedDt_pPermit, HasFinalDt_pPermit)
# 
# # Make the datasets the same length to bind
# max_rows <- max(nrow(sr_demo_q), nrow(sr_pcoo_q))
# 
# # Expand both datasets to match the max row count
# sr_demo_q <- sr_demo_q %>%
#   mutate(row_id = row_number()) %>%
#   complete(row_id = 1:max_rows) %>%
#   select(-row_id)
# 
# sr_pcoo_q <- sr_pcoo_q %>%
#   mutate(row_id = row_number()) %>%
#   complete(row_id = 1:max_rows) %>%
#   select(-row_id)
# 
# # Bind them side by side
# sr_combined <- bind_cols(sr_demo_q, sr_pcoo_q)%>%
#   mutate(disaster_date = as.Date("2017-10-08"))
# 
# # Creating the time intervals
# time_intervals <- seq(3, 7.5*12, by = 3)
# 
# # Counting by quarter after disaster
# sr_q <- sr_combined %>%
#   mutate(ID = row_number()) %>%
#   crossing(interval = time_intervals) %>%
#   mutate(end_date = disaster_date %m+% months(interval))%>%
#   group_by(interval,end_date)%>%
#   summarise(
#     num_cleared = sum(debris_clearance <= end_date, na.rm = TRUE),
#     num_permit_issued = sum(HasIssuedDt_pPermit <= end_date, na.rm = TRUE),
#     num_coo_issued = sum(HasFinalDt_pPermit <= end_date, na.rm = TRUE)
#   ) %>%
#   ungroup()

# ----------------------PRINTING RESULTS----------------------------------------

# Printing out the monthly analysis
print_list <- list(carr = shasta_m, boulder = boulder_m, louisville = louisville_m, superior = superior_m,
                   maui = maui_m, butte = butte_m, paradise = paradise_m, marshall = marshall_m, camp = camp_m)

for (name in names(print_list)){
  write_csv(print_list[[name]], file = paste0("data-intermediate/", name, " monthly stats.csv"))
}



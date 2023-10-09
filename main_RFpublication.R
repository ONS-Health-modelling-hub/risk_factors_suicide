#import required packages
library(dplyr)
library(sparklyr)
library(tidyverse)
library(lubridate)
library(stringr)
library(magrittr)
library(stringi)
library(survival)
library(splines)
library(purrr)
library(plyr)


options(max.print = 9999)

#--------------------------
# Set up the spark connection
#--------------------------

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "RFsuicides_data",
                    config = config,
                    version = "2.3.0")

project_directory <- "<directory_location>"

source(paste0(project_directory, "/", "Functions.R"))


#study start and end dates
start_date = "2011-03-28"
end_date = "2021-12-31" #set DOD to allow a year from current date due to delays in registration

# read in data
df_hes_phda <- sdf_sql(sc, "SELECT * FROM <import data>")

sample_flow_file_name <- "sample_flow_"

# select Census and deaths sources only 
df_hes_phda %<>%
  select(ends_with("_census"), ends_with("_deaths"), final_nhs_number, region_code_census, cen_pr_flag)

sample_flow <- tibble(stage = "total sample",
                      count = sdf_nrow(df_hes_phda))

df_hes_phda %<>%
  filter(cen_pr_flag == 1 & !is.na(final_nhs_number) & uresindpuk11_census == 1) 

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "sample which were Census respondents",
                          count = sdf_nrow(df_hes_phda)))

#remove nonsense dods
df_hes_phda %<>% 
  mutate(doddy_deaths = if_else(is.na(doddy_deaths), "99999",doddy_deaths)) %>%
  mutate(dodmt_deaths = if_else(is.na(dodmt_deaths), "99999",dodmt_deaths)) %>% 
  filter(doddy_deaths != "00" | dodmt_deaths != "00")

#filter people who are alive, or dod > day after census
df_hes_phda %<>%
 filter(is.na(dod_deaths) | (!is.na(dod_deaths) & dod_deaths >= start_date))

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "sample which were alive on Census day and dod < end date",
                          count = sdf_nrow(df_hes_phda)))

#flag for deaths - death is when dod is not NA & dod < study end date
df_hes_phda %<>%
  mutate(death = ifelse(!is.na(dod_deaths) & dod_deaths <= end_date, 1, 0)) %>%
  mutate(underlying_code = str_sub(fic10und_deaths, 1, 3))

#read in look up for causes of death 
death_lookup <- read_csv("path to file")

df_hes_phda %<>%
  left_join(death_lookup, by = c("underlying_code" = "Code"), copy = T) %>% 
  mutate(cause_death = case_when(death == 1 & Type_of_death == "Suicide" ~ "Death_Suicide", 
                                 death == 1 & is.na(Type_of_death) ~ "Death_Other", 
                                 death == 0 ~ "Alive"))
  

#calcualte age at time of census (filter on this as we only want 18 - 74)
df_hes_phda %<>%
  mutate(age_on_cd = datediff(start_date, dob_census)/365.25) %>%
  mutate(age_at_tod = ifelse(death == 1, datediff(dod_deaths, dob_census)/365.25, NA)) %>%
  filter(age_on_cd >= 18 & age_on_cd <= 74)

df_hes_phda %>% group_by(cause_death) %>% count() %>% print()

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "sample which were 18 to 74 on Census day",
                          count = sdf_nrow(df_hes_phda)))

##plot total number of suicides by month over time
  total_events <- df_hes_phda %>%
    filter(cause_death == "Death_Suicide") %>%
    mutate(dod_month_year = substr(dod_deaths, 1, 7)) %>%
    mutate(dod_year = substr(dod_deaths, 1, 4)) %>%
    select(cause_death, dod_month_year, dod_year) %>%
    group_by(dod_month_year) %>%    
    summarize(count = n())  

total_events %<>% arrange(dod_month_year) %>% collect()
                 
p <- ggplot(total_events, aes(x=dod_month_year, y=count)) + geom_bar(stat='identity')

p


## select vars to keep
df_hes_phda %<>% 
   select("aethpuk11_census", "aggcobpuk113_census", "aggethhuk11_census",
    "aggmainlangprf11_census", "afind11_census", "sizhuk11_census",       
    "chlqpuk111_census", "hlqpuk11_census","marstat_census", 
    "relpuk11_census", "disability_census", "nsshuk11_census",
    "sex_census", "age_on_cd", "final_nhs_number", 
    "dod_deaths", "cause_death","age_at_tod", "dob_census","death",
    "region_code_census", "lsoa_code_census") 
      
## recode characteristics
df_hes_phda %<>% 
  mutate(ethnicity = case_when(aethpuk11_census == 1 ~ "White", 
                              aethpuk11_census == 2 ~ "Mixed/multiple ethnic groups",
                              aethpuk11_census == 3 ~ "Indian",
                              aethpuk11_census == 4 ~ "Pakistani and other Asian/Asian British",
                              aethpuk11_census == 5 ~ "Chinese",
                              aethpuk11_census == 6 ~ "Caribbean, African, Black British and other Black",
                              aethpuk11_census == 7 ~ "Arab",
                              aethpuk11_census == 8 ~ "Other ethnic group",
                              aethpuk11_census == "X" ~ "Unknown", 
                              TRUE ~ "Unknown"),
        cob = case_when(aggcobpuk113_census == 1 ~ "Born within the UK", 
                              aggcobpuk113_census == 2 ~ "Born outside the UK",
                              aggcobpuk113_census == "X" ~ "Unknown",
                              TRUE ~ "Unknown"),
        ethnicity_short = case_when(aggethhuk11_census == 1 ~ "White", 
                              aggethhuk11_census == 2 ~ "Mixed/multiple ethnic groups",
                              aggethhuk11_census == 3 ~ "Asian/Asian British",
                              aggethhuk11_census == 4 ~ "Black/African/Caribbean/Black British",
                              aggethhuk11_census == 5 ~ "Other ethnic group",
                              aggethhuk11_census == "X" ~ "Unknown", 
                              TRUE ~ "Unknown"), 
        main_language = case_when(aggmainlangprf11_census == 1 ~ "Main language is English (English or Welsh in Wales)", 
                              aggmainlangprf11_census == 2 | 
                              aggmainlangprf11_census == 3 ~ "Main language is not English (English or Welsh in Wales)",
                              aggmainlangprf11_census == "X" ~ "Unknown",
                              TRUE ~ "Unknown"),   
        armed_forces = case_when(afind11_census == 1 |
                              afind11_census == 3 ~ "Member of armed forces",
                              afind11_census == 2 | 
                              afind11_census == 4 ~ "Dependent (Spouse, same-sex civil partner, partner, child or stepchild) of member of armed forces",
                              afind11_census == "X" ~ "Unknown",
                              TRUE ~ "Not member of dependent of armed forces"),
        household_size = case_when(sizhuk11_census == 01 ~ "1 person in household",
                              sizhuk11_census == 02 ~ "2 people in household",
                              sizhuk11_census >= 03 & sizhuk11_census <= 05 ~ "3 to 5 people in household",
                              sizhuk11_census > 05  ~ "6 people in household",
                              TRUE ~ "Unknown"),
        qualifications_short = case_when(hlqpuk11_census == 15 ~ "Has degree or above",
                              hlqpuk11_census >= 11 & hlqpuk11_census <= 14 ~ "Has qualifications",          
                              hlqpuk11_census == 10 ~ "No qualifications",
                              hlqpuk11_census == "X" ~ "Unknown",
                              TRUE ~ "Unknown"),  
        qualifications = case_when(hlqpuk11_census == 10 ~ "No academic or professional qualifications",
                               hlqpuk11_census == 11 ~ "Level 1",
                               hlqpuk11_census == 12 ~ "Level 2",
                               hlqpuk11_census == 13 ~ "Apprenticeship",
                               hlqpuk11_census == 14 ~ "Level 3",
                               hlqpuk11_census == 15 ~ "Level 4",
                               hlqpuk11_census == 16 ~ "Other", 
                               hlqpuk11_census == "XX" ~ "Unknown",
                               TRUE ~ "Unknown"),
        marital = case_when(marstat_census == 1 ~ "Single (never married or never registered a same-sex civil partnership)",
                            marstat_census == 2 ~ "Married",
                            marstat_census == 3 ~ "Separated (but still legally married)",
                            marstat_census == 4 ~ "Divorced",
                            marstat_census == 5 ~ "Widowed",
                            marstat_census == 6 ~ "In a registered same-sex civil partnership",
                            marstat_census == 7 ~ "Separated (but still legally in a same-sex civil partnership)",
                            marstat_census == 8 ~ "Formerly in a same-sex civil partnership which is now legally dissolved",
                            marstat_census == 9 ~ "Surviving partner from a same-sex civil partnership",
                            TRUE ~ "Unknown"),
         marital_short = case_when(marstat_census == 1 ~ "Single",
                            marstat_census == 2 | 
                            marstat_census == 6 ~ "In partnership",
                            marstat_census == 5 | 
                            marstat_census == 9 ~ "Partner deceased",
                            marstat_census == 3 | 
                            marstat_census == 7 | 
														marstat_census == 8 | 
														marstat_census == 4 ~ "Separated",
                            TRUE ~ "Unknown"),
        religion = case_when(relpuk11_census == 1 ~ "No religion",
                             relpuk11_census == 2 ~ "Christian",
                             relpuk11_census == 3 ~ "Buddhist",
                             relpuk11_census == 4 ~ "Hindu",
                             relpuk11_census == 5 ~ "Jewish",
                             relpuk11_census == 6 ~ "Muslim",
                             relpuk11_census == 7 ~ "Sikh",
                             relpuk11_census == 8 ~ "Other",
                             relpuk11_census == 9 ~ "Not stated",
                             relpuk11_census == "X" ~ "Unknown",
                             TRUE ~ "Unknown"),
         region = case_when(region_code_census == "E12000001" ~ "North East", 
                             region_code_census == "E12000002" ~ "North West", 
                             region_code_census == "E12000003" ~ "Yorkshire and the Humber ", 
                             region_code_census == "E12000004" ~ "North Midlands", 
                             region_code_census == "E12000005" ~ "West Midlands", 
                             region_code_census == "E12000006" ~ "East of England", 
                             region_code_census == "E12000007" ~ "London", 
                             region_code_census == "E12000008" ~ "South East", 
                             region_code_census == "E12000009" ~ "South West",
                             region_code_census == "W92000004" ~ "Wales",
                             TRUE ~ "Unknown"),
          disability = case_when(disability_census == "1" | 
                             disability_census == "2" ~ "Day to day activities limited a lot/a little",
                             disability_census == "3" ~ "Day to day activities not limited", 
                             TRUE ~ "Unknown"),
          nssec = case_when(nsshuk11_census >= 1 & nsshuk11_census < 3 ~ "Class 1.1",
                             nsshuk11_census >= 3 & nsshuk11_census < 4 ~ "Class 1.2",
                             nsshuk11_census >= 4 & nsshuk11_census < 7 ~ "Class 2",
                             nsshuk11_census >= 7 & nsshuk11_census < 8 ~ "Class 3",                          
                             nsshuk11_census >= 8 & nsshuk11_census < 10 ~ "Class 4",                          
                             nsshuk11_census >= 10 & nsshuk11_census < 12 ~ "Class 5",                          
                             nsshuk11_census >= 12 & nsshuk11_census < 13 ~ "Class 6",                          
                             nsshuk11_census >= 13 & nsshuk11_census < 14 ~ "Class 7",
                             nsshuk11_census >= 14 & nsshuk11_census < 15 ~ "Class 8",
                             nsshuk11_census == 15 ~ "Students", 
														 nsshuk11_census > 15 ~ "Not classifiable", 
                             TRUE ~ "Unknown"), 
           sex = case_when(sex_census == 1 ~ "Male", 
                             sex_census == 2 ~ "Female", 
                             TRUE ~ "Unknown"))

df_hes_phda %>% 
	group_by(nsshuk11_census, nssec) %>%
	count() 

df_hes_phda %<>%
  mutate(age_group = case_when(age_on_cd >= 18 & age_on_cd < 30 ~ "18-29",
                               age_on_cd >= 30 & age_on_cd < 50 ~ "30-49",
                               age_on_cd >= 50 & age_on_cd < 70 ~ "50-69",
                               age_on_cd >= 70 ~ "over 70")) %>%
  mutate(age_group_10 = case_when(age_on_cd >= 18 & age_on_cd < 30 ~ "18-29",
                               age_on_cd >= 30 & age_on_cd < 40 ~ "30-39",
                               age_on_cd >= 40 & age_on_cd < 50 ~ "40-49",
                               age_on_cd >= 50 & age_on_cd < 60 ~ "50-59",
                               age_on_cd >= 60 & age_on_cd < 70 ~ "60-69",  
                               age_on_cd >= 70 & age_on_cd < 80 ~ "70-79", 
                               age_on_cd >= 80 ~ "80 plus")) %>% 
  select(-contains("census"))

df_hes_phda %<>%
  mutate(age_round = floor(age_on_cd))

#create time at risk var for model
df_hes_phda %<>% 
  mutate(time_at_risk = case_when(death == 1 ~ datediff(dod_deaths, start_date),
                                 death == 0 ~ datediff(end_date, start_date))) %>%
  filter(time_at_risk >= 0)

sample_flow <- sample_flow %>%
  dplyr::bind_rows(tibble(stage = "sample which had +ve time at risk",
                          count = sdf_nrow(df_hes_phda)))

#collect to memory and store
write.csv(sample_flow, "<path to store sample flow>")

#create binary outcomes
df_hes_phda %<>%
  mutate(outcome_suicide = ifelse(cause_death == "Death_Suicide", 1, 0)) %>% 
  mutate(outcome_death = ifelse(cause_death == "Alive", 0, 1)) 

#select vars of interst for counts
demog_varsnames <- c("ethnicity",
                    "marital_short",
                    "religion", 
                    "disability",
                    "age_group_10",
										"region", 
                    "armed_forces",
                    "nssec", 
										"sex")

demog_table_suicides <- purrr::pmap_dfr(.l = list(data = purrr::map(1:length(demog_varsnames), function(e) {return(df_hes_phda)}),
                                           by_var = demog_varsnames,
                                           total = purrr::map(1:length(demog_varsnames), function(e) {return(nrow(df_hes_phda))})),
                                          .f = crosstabs_rate_fun) 


demog_table_suicides %>% print()

write.csv(demog_table_suicides, "<path to store descriptive counts>")

##create analytical df #########################################

var_group_names <- c("age_round",
                    "ethnicity",
                    "marital_short",
                    "disability",
                    "sex", 
                    "region", 
                    "nssec", 
                    "religion",
                    "armed_forces")

var_matrix_count <- df_hes_phda %>%
  group_by_at(var_group_names) %>% 
  summarize(count = n(), 
            total_events = sum(outcome_suicide),
            total_time_at_risk_days = sum(time_at_risk)) %>%
  ungroup()

var_matrix_count %<>% collect()

#sense check
nrow(filter(var_matrix_count, count != 0))

## save file as RDS
run_date <- Sys.Date()
run_date <- gsub('-', '_', run_date)

#store to RDS ################################################################
saveRDS(var_matrix_count, paste0("<path/filename>", "_", run_date, ".RDS"))

write.csv(var_matrix_count, paste0("<path and file name>", "_", run_date, ".csv"))

#read in data from memory
data_date <- "2023_02_28"

path = "<project path>"

df_popsum <- read_rds(paste0(path, "<analytical data from memory>", data_date, ".RDS"))

df_popsum %>% head(1000) %>% print()

##update armed forces unknown to no-memmber
df_popsum %<>%
  mutate(armed_forces_member = case_when(armed_forces == "Dependent (Spouse, same-sex civil partner, partner, child or stepchild) of member of armed forces" ~
                                        "Dependent of member of armed forces",
                                        armed_forces == "Member of armed forces" ~
                                        "Member of armed forces", 
                                        armed_forces == "Unknown" ~
                                        "Not member or dependant")) %>%
  select(-armed_forces)


linear <- "+"
interact <- "*"

## set refrence categories 
df_popsum$ethnicity <- as.factor(df_popsum$ethnicity)
df_popsum$armed_forces_member <- as.factor(df_popsum$armed_forces_member)
df_popsum$marital_short <- as.factor(df_popsum$marital_short)
df_popsum$religion <- as.factor(df_popsum$religion)
df_popsum$region<- as.factor(df_popsum$region)
df_popsum$disability <- as.factor(df_popsum$disability)
df_popsum$nssec <- as.factor(df_popsum$nssec)

df_popsum$ethnicity <- relevel(df_popsum$ethnicity, ref = "White")
df_popsum$armed_forces_member <- relevel(df_popsum$armed_forces_member, ref = "Not member or dependant") ## no TRUE category
df_popsum$marital_short <- relevel(df_popsum$marital_short, ref = "Single")
df_popsum$religion <- relevel(df_popsum$religion, ref = "No religion")
df_popsum$region <- relevel(df_popsum$region, ref = "London")
df_popsum$disability <- relevel(df_popsum$disability, ref = "Day to day activities not limited")
df_popsum$nssec <- relevel(df_popsum$nssec, ref = "Class 1.1")



#total number of df for age spline to test in the model ##################################################################

max_df = 12

formula_list <- list()

for (i in 1:max_df){
  
  formula <- paste0('total_events ~ sex * ns(age_round, df=', 
              i, 
              ', Boundary.knots=quantile(age_round, c(.01, .99)))', 
              " + offset(log(total_time_at_risk_days/365.25))")
  
  formula_list <- c(formula_list, formula)
  
}


bic_list <- list()  
age_sex_glm <- list()


for (i in 1:max_df){
age_sex_glm$model[[i]] <- glm(formula = formula_list[[i]],
      family = poisson(link = "log"),
       data = df_popsum) 
bic <- (BIC(age_sex_glm$model[[i]]))
bic_list[[i]] <- bic

}


min_df <- which.min(bic_list)

## returns no of df for age spline internal knots
print(min_df) 

##age and sex only model ##################################################################
linear_age_sex <- paste0('total_events ~ sex + ns(age_round, df=',
                          min_df, 
                          ', Boundary.knots=quantile(age_round, c(.01, .99))) + offset(log(total_time_at_risk_days/365.25))')

inter_age_sex <- paste0('total_events ~ sex * ns(age_round, df=',
                          min_df, 
                          ', Boundary.knots=quantile(age_round, c(.01, .99))) + offset(log(total_time_at_risk_days/365.25))')

model_linear_age_sex <- glm(formula = linear_age_sex,
                               family = poisson(link = "log"),
                               data = df_popsum)

model_inter_age_sex <- glm(formula = inter_age_sex,
                               family = poisson(link = "log"),
                               data = df_popsum)

sex_age_anova  <- anova(model_linear_age_sex, model_inter_age_sex, test = "LRT") %>%
                    as.data.frame() %>%
                    mutate(model = c("linear", "interacting")) %>% 
                    mutate(AIC = c(AIC(model_linear_age_sex), AIC(model_inter_age_sex))) %>% 
                    mutate(BIC = c(BIC(model_linear_age_sex), BIC(model_inter_age_sex)))


#model fit measures
print(sex_age_anova)

#create rates for age and sex 

emmeans_sex_age <- emmeans::emmeans(model_linear_age_sex,
                    offset = 0,
                    specs = c("sex"),
                    data = df_popsum,
                    type = "response",
                    weights = "proportional") %>%
          as.data.frame() %>%
          dplyr::rename(rate_lci = asymp.LCL,
                  rate_uci = asymp.UCL) %>%
          dplyr::select(-df) %>%
          dplyr::mutate(across(
            .cols = starts_with("rate"),
            .fns = ~ . * 100000)) %>% 
          dplyr::mutate(model = "linear")

  temp <- emmeans::emmeans(model_inter_age_sex,
                      offset = 0,
                      specs = c("sex"),
                      data = df_popsum,
                      type = "response",
                      weights = "proportional") %>%
            as.data.frame() %>%
            dplyr::rename(rate_lci = asymp.LCL,
                    rate_uci = asymp.UCL) %>%
            dplyr::select(-df) %>%
            dplyr::mutate(across(
              .cols = starts_with("rate"),
              .fns = ~ . * 100000)) %>% 
            dplyr::mutate(model = "interacting")
          
emmeans_sex_age <- rbind(emmeans_sex_age, temp)          
                    

write.csv(emmeans_sex_age, paste0(path, "emmeans_out_age_sex", ".csv"))

##all ages estimates seperately for the interacting plot

emmeans_all_age_sex <- model_inter_age_sex %>% 
          emmeans::ref_grid(
          data = df_popsum,
          at = list(age_round = unique(df_popsum$age_round))) %>%    
        emmeans::emmeans(
          object = .,
          offset = 0,
          specs = c("age_round",
                    "sex"),
          data = df_popsum,
          type = "response",
          weights = "proportional") %>%
        as.data.frame() %>%
        dplyr::rename(rate_lci = asymp.LCL,
          rate_uci = asymp.UCL) %>%
        dplyr::select(-df) %>%
        dplyr::mutate(across(
          .cols = starts_with("rate"),
          .fns = ~ . * 100000)) 


write.csv(emmeans_all_age_sex, paste0(path, "emmeans_out_age_sex_allplot", ".csv"))

p <- ggplot(data = emmeans_all_age_sex, 
                aes(x = age_round, y = rate, colour = sex)) + 
    geom_ribbon(data = emmeans_all_age_sex,
                aes(ymin = rate_lci, ymax = rate_uci), alpha=0.15, show.legend=FALSE) +
    geom_line() +
    theme(legend.title = element_blank()) +
    ylab("Rate of suicide per 100,000 people") + xlab("Age (years)") + 
    theme_bw() 
    
p

ggsave(paste0(path, "/plots/age_sex_interactingplot.jpg"))


##list exposures of interest ##################################################################
##list of inputs to create models
exposure <- c("ethnicity",
               "marital_short",
               "disability",
               "region", 
               "nssec",
               "religion",
               "armed_forces_member")


for (i in 1:length(exposure)){
  
  # option 1: (sex * age) + (sex * exposure) + offset
  m1 <- paste0('total_events ~ (sex * ns(age_round, df=', 
              min_df, 
              ', Boundary.knots=quantile(age_round, c(.01, .99))))', 
              " ", linear, " ",
              "(sex * ", exposure, 
              ") + offset(log(total_time_at_risk_days/365.25))")
  
  #option 2: sex * age * exposure + offset
  m2 <- paste0('total_events ~ sex * ns(age_round, df=', 
              min_df, 
              ', Boundary.knots=quantile(age_round, c(.01, .99)))', 
              " ", interact, " ",
              exposure, 
              " + offset(log(total_time_at_risk_days/365.25))")   
}


model_linear <- list()
model_interacting <- list()

for (i in 1:length(m1)){
  
model_linear[[i]] <- glm(formula = m1[[i]],
                               family = poisson(link = "log"),
                               data = df_popsum)
                               
                               ##add in tidy models here?

model_interacting[[i]] <- glm(formula = m2[[i]],
                               family = poisson(link = "log"),
                               data = df_popsum)
                               
                               
}


anova_m1_m2 <- Map(anova, model_linear, model_interacting, test = "LRT") 
BIC_m1_m2 <- Map(BIC, model_linear, model_interacting)

#testing function
#temp <- emmeans_estimates(model_linear[[1]], "ethnicity")

#purrr loop for all linear outputs
emmeans_linear_output <- purrr::pmap_dfr(.l = list(model = model_linear,
                                           variable = exposure),
                                          .f = emmeans_estimates) %>% 
      dplyr::mutate(model = "linear") %>% 
      dplyr::mutate(non_conf_rate = ifelse(total_count >= 30, 1, 0)) %>% 
      dplyr::mutate(rate_est = ifelse(non_conf_rate == 1, round(rate, 2), "[C]"),
                  rate_lci_est = ifelse(non_conf_rate ==1, round(rate_lci, 2), "[C]"),
                  rate_uci_est = ifelse(non_conf_rate == 1, round(rate_uci, 2), "[C]"))
  

emmeans_inter_output <- purrr::pmap_dfr(.l = list(model = model_interacting,
                                           variable = exposure),
                                          .f = emmeans_estimates) %>%
      mutate(model = "interacting") %>% 
      dplyr::mutate(non_conf_rate = ifelse(total_count >= 30, 1, 0)) %>% 
      dplyr::mutate(rate_est = ifelse(non_conf_rate == 1, round(rate, 2), "[C]"),
                  rate_lci_est = ifelse(non_conf_rate ==1, round(rate_lci, 2), "[C]"),
                  rate_uci_est = ifelse(non_conf_rate == 1, round(rate_uci, 2), "[C]"))


#bind linear and interacting models and save
emmeans_out <- rbind(emmeans_linear_output, emmeans_inter_output) 

write.csv(emmeans_out, paste0(path, "emmeans_out", ".csv"))


#all ages estimate from linear model ##################################################################

#purrr loop for all linear outputs
emmeans_average_output <- purrr::pmap_dfr(.l = list(model = model_linear,
                                           variable = exposure),
                                          .f = emmeans_average)  %>%
                          dplyr::mutate(model = c("linear")) %>% 
                          #disability status is best explained by the inter model
                          dplyr::filter(exposure != "disability")

emmeans_average_inter_output <- purrr::pmap_dfr(.l = list(model = model_interacting,
                                           variable = exposure),
                                          .f = emmeans_average) %>%
                           dplyr::mutate(model = c("interacting")) %>% 
                           filter(exposure == "disability")


ememeans_average <- rbind(emmeans_average_output, emmeans_average_inter_output)


write.csv(ememeans_average, paste0(path, "emmeans_average", ".csv"))


##loop to plot heat maps ###########################################################################

exposure <- c("ethnicity",
               "marital_short",
               "disability",
               "region", 
               "nssec",
               "religion",
               "armed_forces_member")

exposure_title <- c("Ethnicity",
               "Marital status",
               "Disability status",
               "Region", 
               "NS-SEC",
               "Religion",
               "Armed forces")


#select ages to plot
age_list <- c(20, 30, 40, 50, 60, 70)

#select reduced number of ages and average for armed forces estimates
age_list_armedforces <- c(20, 30, 40, 50, "Average")

#specify linear or interacting model to plot and save
heatmap_plot <- emmeans_out %>%
  filter(model == "linear") %>%
#  filter(model == "interacting") %>%
  select(-non_conf_rate, -total_count) %>%
  filter(age_round %in% age_list)

heatmap_plot$rate_est <- as.numeric(heatmap_plot$rate_est)

emmeans_average_plot <- emmeans_average_output %>%
  dplyr::mutate(rate_est = round(rate, 2),
                  rate_lci_est = round(rate_lci, 2),
                  rate_uci_est = round(rate_uci, 2))

heatmap_plot <- rbind(heatmap_plot, emmeans_average_plot)

for (i in 1:length(exposure)){
  
  exp_plot <- exposure[[i]]

  #add in catch to only ploy age 20, 30, 40, 50 for armed forces
  if (exp_plot == "armed_forces_member"){
    
    heatmap_plot <- heatmap_plot %>%
      filter(age_round %in% age_list_armedforces) 
    
  } else{
    
  }  

  ggplot(data = filter(heatmap_plot, exposure == exp_plot), 
       aes(x = age_round, y = group, fill = rate_est))  +
    geom_tile(colour = "white") + 
    scale_fill_gradient2(low = "yellow", high = "red") +
    theme_minimal() +  
    geom_text(aes(label = rate_est), size = 2) + 
    labs(title= paste0("Rates of suicide per 100,000: \n", exposure_title[[i]])) +
    ylab(" ") + xlab("Age (years)") +
    facet_wrap(vars(sex)) + 
    theme_bw()
  
ggsave(paste0(path, "/plots/", "linear_heatmap_", exposure[[i]], ".jpg"))
#ggsave(paste0(path, "/plots/", "interacting_heatmap_", exposure[[i]], ".jpg"))

}


#loop for model plots with age #######################################################

for(i in 1:length(exposure)){
  
  exp_plot <- exposure[[i]]
  
  plot <- emmeans_out %>% 
#    filter(model == "linear" & exposure == exp_plot) %>% 
    filter(model == "interacting" & exposure == exp_plot) %>%
    filter(rate_est != "[C]")

  plot$rate_est <- as.numeric(plot$rate_est)
  plot$rate_lci_est <- as.numeric(plot$rate_lci_est)
  plot$rate_uci_est <- as.numeric(plot$rate_uci_est)

  #get an error for armed forces plotting (filter age < 50)

  ggplot(data = plot,
           aes(x = age_round, y = rate_est, colour = group)) +
    geom_ribbon(data = plot,
                aes(ymin = rate_lci_est, ymax = rate_uci_est), alpha=0.15, show.legend=FALSE) +
    geom_line() +
    labs(title= paste0("Rates of suicide per 100,000: \n", exposure_title[[i]])) +
    facet_wrap(vars(sex)) +
    theme_bw() + 
    ylab("Suicide rate per 100,000 people") + xlab("Age (years)") 

  #only want to store the interacting line plot for disability status
  if (exposure[[i]] == "disability"){
  #ggsave(paste0(path, "/plots/linear_lineplot_", exposure[[i]], ".jpg"))  
  ggsave(paste0(path, "/plots/interacting_lineplot_", exposure[[i]], ".jpg"))
  } else{

  }

}

##comparrison of ORs            
##fully adjusted model ##################################################################

##(sex * (exp1 + exp2 +))
fully_adjusted <- paste0('total_events ~ ns(age_round, df=',
                          min_df, 
                          ', Boundary.knots=quantile(age_round, c(.01, .99)))  * sex + sex * (',
                          'ethnicity + ',
                          'marital_short + ',
                          'disability + ',
                          'region + ',
                          'nssec + ',
                          'religion + armed_forces_member) +',
                          ' offset(log(total_time_at_risk_days/365.25))')


model_fully_adjusted <- glm(formula = fully_adjusted,
                               family = poisson(link = "log"),
                               data = df_popsum)
                               


## Calculate ORs for 1. (age*sex) + (sex*exposure) vs 2. (age*sex)+(sex * (exp1 + exp2 +))



 
###this works - need to update with M/F labels etc
#test <- calulating_OR_inter(model_input = model_linear[[1]], 
#                            exposure = "ethnicity",
#                            ref_level = "Male", 
#                            interaction_var = "sex")


exposure <- c("ethnicity",
               "marital_short",
               "disability",
               "region", 
               "nssec",
               "religion", 
               "armed_forces_member")

interaction_vars <- c("sex")
ref_levels <- list("Male")

OR_minimal <- purrr::pmap_dfr(.l = list(interaction_var = interaction_vars,
                                         ref_level = ref_levels,
                                         model_input = model_linear,
                                         exposure = exposure),
                               .f = calulating_OR_inter) %>%
  dplyr::mutate(adjustment = c("minimal"))



OR_fully_inter <- purrr::pmap_dfr(.l = list(interaction_var = interaction_vars,
                                         ref_level = ref_levels,
                                         model_input = purrr::map(1:length(exposure), function(e) {return(model_fully_adjusted)}),
                                         exposure = exposure),
                               .f = calulating_OR_inter) %>%
  dplyr::mutate(adjustment = c("full"))

OR_join <- full_join(OR_minimal, OR_fully_inter, by = c("term", "model"))

write.csv(OR_join, paste0(path, "ORs_minimal_adjusted.csv"))



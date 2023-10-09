#' @title cross tabs of outcomes
#' 
#' Create counts of outcomes by variable
#'
#' @param data Dataframe
#' @param by_var List of variables for which to create the descriptive tables
#' @param total Length of the list of the variable list
#' @return table with descriptive statistics

crosstabs_rate_fun <- function(data, by_var, total) {
  crosstabs_table <- data %>%
  group_by(.data[[by_var]]) %>%
  count() %>%
  rename(group = all_of(by_var)) %>%
  mutate(group = as.character(group),
           domain = by_var) %>%
  ungroup() %>%
  select(group, domain, n) 
  
  suicides_rates <- data %>% 
  group_by(.data[[by_var]], cause_death) %>% 
  count() %>%
  rename(group = all_of(by_var)) %>%
  filter(cause_death == "Death_Suicide" | cause_death == "Death_Other") %>%
  rename(totalevents = n)
  
  jointable <- merge(crosstabs_table, suicides_rates) %>%
  select(group, n, cause_death, totalevents) %>%
  pivot_wider(names_from = cause_death, values_from = totalevents) %>%
  rename(totalsample = n) %>%
  mutate(Death_AllCause = Death_Suicide + Death_Other,
         Rate_Suicide = round(Death_Suicide/totalsample*100000,2),
         Rate_AllCause = round(Death_AllCause/totalsample*100000,2)) %>%
  select(-Death_Other)
  
}

#' @title emmeans model estimates for each level of var for different ages
#' 
#' est rates from poisson model
#'
#' @param model input model
#' @param variable string of var name

emmeans_estimates <- function(model, variable){
    
  data_temp <- df_popsum %>% 
    select(sex, age_round, variable, count, 
           total_events, total_time_at_risk_days)

  names(data_temp)[names(data_temp) == variable] <- "predictor"

  data_agg <- plyr::ddply(data_temp,.(age_round, sex, predictor), 
                           summarise, total_count = sum(count))
  
  data_agg <- data_agg %>% 
    dplyr::rename(group = predictor)
  
   emmeans_group <- model %>%
        emmeans::ref_grid(
          data = df_popsum,
          at = list(age_round = unique(df_popsum$age_round))) %>%    
        emmeans::emmeans(
          object = .,
          offset = 0,
          specs = c("age_round",
                    "sex",
                    variable),
          data = df_popsum,
          type = "response",
          weights = "proportional") %>%
        as.data.frame() %>%
        dplyr::rename(rate_lci = asymp.LCL,
          rate_uci = asymp.UCL) %>%
        dplyr::select(-df) %>%
        dplyr::mutate(across(
          .cols = starts_with("rate"),
          .fns = ~ . * 100000), 
          exposure = variable) %>%
        dplyr::rename(group = all_of(variable))
  
   emmeans_group_flag <- full_join(emmeans_group, data_agg, by = c("age_round",
                    "sex",
                    "group"))

  return(emmeans_group_flag)
  
}

#' @title emmeans model estimates for each level av across all
#' 
#' est rates averaged across all ages
#'
#' @param model input model
#' @param variable string of var name
#' 
emmeans_average <- function(variable, model){

  df_group <- emmeans::emmeans(model,
                    offset = 0,
                    specs = c("sex",
                              variable),
                    data = df_popsum,
                    type = "response",
                    weights = "proportional") %>%
          as.data.frame() %>%
          dplyr::rename(rate_lci = asymp.LCL,
                  rate_uci = asymp.UCL) %>%
          dplyr::select(-df) %>%
          dplyr::mutate(across(
            .cols = starts_with("rate"),
            .fns = ~ . * 100000),
            exposure = variable, 
            age_round = "Average") %>% 
          dplyr::rename(group = !!variable)
             
}

#' @title emmeans model estimates for each level av across all
#' 
#' est rates averaged across all ages
#'
#' @param model input model
#' @param variable string of var name
#' 
#' 
calulating_OR_inter <- function(model_input, exposure, ref_level, interaction_var){
  
    model <- model_input

    modelparams <- names(coefficients(model))

    modelparams <- grep(pattern = paste0(interaction_var, "|", exposure),
                          x = modelparams,
                          value = TRUE)
  
    #remove the sex and age interaction
    toRemove <- grep("age_round", modelparams)
  
    modelparams <- modelparams[-toRemove]

      tidymodel <- tidy(model_input)               
      tidymodel <- filter(tidymodel, term %in% modelparams)                   
      tidymodel$model <- c(exposure)
      tidymodel$variance <- (tidymodel$std.error)^2
      varmodel <- vcov(model)

    toRemoveSex <- grep("sex", modelparams)
  
    levels_without_ref <- modelparams[-toRemoveSex]
        
    temp_tidymodel <- purrr::map_dfr(levels_without_ref, function(e) {
      
      var1 <- e
      var2 <- paste0("sexMale:", var1)
      
      #variance of the coef for varint*vax interation outuput v(b) * v(c) + 2 cov(b,c)                 
      intervar = varmodel[var1, var1] + varmodel[var2, var2] + 2*(varmodel[var1, var2])
                                                                      
      temp_tidymodel <- tibble(term = paste0(var2, "_OR"),
                               estimate = sum(tidymodel[tidymodel$term == var1, ]$estimate,
                                              tidymodel[tidymodel$term == var2, ]$estimate),
                               variance = abs(intervar),
                               model = exposure) %>%
                        mutate(sex = "Male")
    
    })
    
    #all refs to male 
    toRemove <- grep("sex",tidymodel$term)
  
    modelparams_female <- modelparams[-toRemove]
  
    tidymodel <- tidymodel %>% 
      filter(term %in% modelparams_female) %>%
      mutate(sex = c("Female"))
  
    tidymodel <- bind_rows(tidymodel, temp_tidymodel)
   
    tidymodel <- tidymodel %>%
        dplyr::mutate(exp_term = exp(estimate)) %>% 
        dplyr::mutate(std.error = sqrt(variance)) %>%
        dplyr::mutate(CI_lower = exp(estimate - (1.96 * std.error))) %>%
        dplyr::mutate(CI_upper = exp(estimate + (1.96 * std.error))) %>%
        dplyr::select(-estimate, -std.error, -statistic, -p.value)

  
      return(tidymodel)

}

#' @title cox mocdel
#' 
#' estimate cox model and store outcomes
#'
#'
#' @param df Dataframe
#' @param surv String specifying start of model e.g 'Surv(time_at_risk, outcome)'
#' @param weight column in df specifying wieight for sampled data
#' @param controls list of control vars
#' 

estimate_cox <- function(df, surv, weight=NULL, controls, save_model=T){
  
  # estimates models
  # Put check in for weights existing
  if (!is.null(weight)){
  wgt = df[[weight]]
  }else{
    wgt=rep(1, nrow(df))
  }
  
  models <- lapply(controls, function(control){
      cat(paste0("Fit model:", control, "\n \n"))
      m <- coxph(as.formula(paste0(surv, " ~", control)), 
                 data = df,
                 weights = wgt)
      return(m)
  })
 
  # get HR and CI in dataframe
  df_hr_models <- purrr::map_dfr(1:length(models), function(i){
    
    df_hr_model <- broom::tidy(models[[i]], exp=TRUE) %>%
      mutate(model=i)

    hr_ci <- data.frame(confint.default(models[[i]]))
    colnames(hr_ci) <- c("conf.low", "conf.high")
    hr_ci <- hr_ci %>%
      mutate(conf.low = exp(conf.low),
             conf.high = exp(conf.high))
      
    df_hr_model <- bind_cols(df_hr_model, hr_ci)


    return(df_hr_model)
  
  })
  
  conc <- sapply(models, function(m){summary(m)$concordance[[1]]})
    
  diagnostics <- data.frame(model=1:length(models), concordance = conc)
 
  ## Output
  # create empty list to store results
  output <- list()
  
  # return the models
  if(save_model == TRUE){
  output$models <- models 
  }
  # return the HR
  output$hr <-  df_hr_models
  
  # diagnostics
  output$diagnostics <- diagnostics

  return(output)
}


get_crude_rates <- function(df, df_poisson, group_vars){
  
  # group_vars : list of socio-demographic variables to calculate ASMRs by. E.g c("ethnicity", "country_of_birth"). List must be of at least length 1.

  df_rates <- df %>%
    mutate(count = round(count / 5) * 5,
           population = round(population / 5) * 5) %>%
    group_by(across(all_of(!!group_vars))) %>%
    summarise(across(c(population, count), sum, na.rm=TRUE)) %>%
    ungroup() %>%
    mutate(crude_rate = count / population) %>%
    mutate(crude_variance = crude_rate * (1-crude_rate),
           crude_standard_error = sqrt(crude_variance / population)) %>%
    mutate(crude_lower_ci = crude_rate - (1.96 * crude_standard_error),
           crude_upper_ci = crude_rate + (1.96 * crude_standard_error)) %>%
    mutate(across(.cols = c(crude_rate, crude_lower_ci, crude_upper_ci), .fns = ~ .x * 100)) %>%
    mutate(crude_lower_ci = ifelse(crude_lower_ci < 0, 0, crude_lower_ci),
           crude_upper_ci = ifelse(crude_upper_ci > 100, 100, crude_upper_ci))

  df_rates <- df_rates %>%
    select(all_of(group_vars), population, count, crude_rate, crude_lower_ci, crude_upper_ci) %>%
    mutate(age_standardised_rate = NA_real_,
           age_standardised_lower_ci = NA_real_,
           age_standardised_upper_ci = NA_real_)
  
  return(df_rates)
  
}

get_asmrs <- function(df, df_esp, df_poisson, min_age, max_age, group_vars){
  
  # group_vars : list of socio-demographic variables to calculate ASMRs by. E.g c("ethnicity", "country_of_birth"). List must be of at least length 1.

#-------------------------------
# Augment ESPs to fit age groups
#-------------------------------

  df_rates <- left_join(df, df_esp, by = "age_group_esp") %>%
    # Filter age bands that are below min age or above max age
    mutate(age_group_lower = unlist(purrr::map(strsplit(age_group_esp, "-", fixed=TRUE), 1)),
           age_group_lower = gsub("+", "", age_group_lower, fixed=TRUE),
           age_group_lower = gsub("<", "", age_group_lower, fixed=TRUE),
           age_group_lower = as.integer(age_group_lower),
           age_group_lower = ifelse(age_group_esp == "<1", 0, age_group_lower),
           age_group_upper = unlist(purrr::map(strsplit(age_group_esp, "-", fixed=TRUE), last)),
           age_group_upper = gsub("+", "", age_group_upper, fixed=TRUE),
           age_group_upper = gsub("<", "", age_group_upper, fixed=TRUE),
           age_group_upper = ifelse(age_group_esp == "<1", as.integer(age_group_upper), as.integer(age_group_upper) + 1),
           age_group_upper = ifelse(age_group_esp == "90+", 91, age_group_upper)) %>%
    filter(age_group_lower >= min_age & age_group_upper <= max_age) %>%
    select(!c(age_group_lower, age_group_upper))

  df_rates <- df_rates %>%
    mutate(count = round(count / 5) * 5,
           population = round(population / 5) * 5) %>%
    mutate(rate_per_100000 = count / population * 100000,
           variance = ((rate_per_100000^2) / count) * (ESP^2),
           standardised_rate = rate_per_100000 * ESP)
  if (sum(is.na(df_rates$ESP)) != 0){
    stop("Missing population weights. Check the labels of the age groups")
  }

  df_rates <- df_rates %>%
    group_by(across(all_of(!!group_vars))) %>%
    summarise(across(c(population, count, standardised_rate, variance, ESP), sum, na.rm=TRUE)) %>%
    ungroup() %>%
    left_join(., df_poisson, by = c("count" = "Deaths")) %>%
    mutate(crude_rate = count / population) %>%
    mutate(crude_variance = crude_rate * (1-crude_rate),
           crude_standard_error = sqrt(crude_variance / population)) %>%
    mutate(crude_lower_ci = crude_rate - (1.96 * crude_standard_error),
           crude_upper_ci = crude_rate + (1.96 * crude_standard_error)) %>%  
    mutate(age_standardised_rate = (standardised_rate / ESP),
           age_standardised_variance = variance / (ESP^2),
           age_standardised_standard_error = sqrt(age_standardised_variance)) %>%
    mutate(age_standardised_lower_ci = ifelse(count >= 100 | count==0,
                                              age_standardised_rate - (1.96 * age_standardised_standard_error),
                                              age_standardised_rate + (L * count - count) * ((age_standardised_variance/count)^0.5)),
           age_standardised_upper_ci = ifelse(count >= 100 | count==0,
                                              age_standardised_rate + (1.96 * age_standardised_standard_error),
                                              age_standardised_rate + (U * count - count) * ((age_standardised_variance/count)^0.5))) %>%
    mutate(across(.cols = c(crude_rate, crude_lower_ci, crude_upper_ci), .fns = ~ .x * 100)) %>%
    mutate(across(.cols = c(age_standardised_rate, age_standardised_lower_ci, age_standardised_upper_ci), .fns = ~ .x / 1000)) %>%
    mutate(crude_lower_ci = ifelse(crude_lower_ci < 0, 0, crude_lower_ci),
           crude_upper_ci = ifelse(crude_upper_ci > 100, 100, crude_upper_ci),
           age_standardised_lower_ci = ifelse(age_standardised_lower_ci < 0, 0, age_standardised_lower_ci),
           age_standardised_upper_ci = ifelse(age_standardised_upper_ci > 100, 100, age_standardised_upper_ci))

  df_rates <- df_rates %>%
    select(all_of(group_vars), population, count, crude_rate, crude_lower_ci, crude_upper_ci, age_standardised_rate, age_standardised_lower_ci, age_standardised_upper_ci)
  
  return(df_rates)
  
}






######################################################################


### Age-Standardised Rates
get_age_standardised_rates <- function(data,
                                       ESP_data = ESP,
                                       poisson_data = poisson, 
                                       total_esp,
                                       outcome,
                                       time_var,
                                       type_of_outcome,
                                       age_grp,
                                       group_vars,
                                       weight = NULL) {
  
  if(!(type_of_outcome %in% c("time to event", "count"))) {
    
    stop(cat("Must specify 'type_of_outcome' as one of:",
             "time to event", "count"))
    
  }
  
  if (missing(weight)) {
    
    data <- data %>%
      ungroup() %>%
      mutate(weight = 1)
    
    weight <- "weight"
    
  }
  
  if (type_of_outcome == "time to event") {
    
    grp_vars = c(age_grp, group_vars)
    
    rates_by_age <- data %>%
      mutate(time_at_risk_years = ((.data[[time_var]] / 365.25) + 1),
             time_at_risk_years_weighted = time_at_risk_years * .data[[weight]],
             outcome_weighted = .data[[outcome]] * .data[[weight]]) %>%
      group_by(across(all_of(!!grp_vars))) %>%
      summarise(unweighted_count = sum(.data[[outcome]], na.rm = TRUE),
                count = sum(outcome_weighted, na.rm = TRUE),
                sample = n(),
                pops = sum(.data[[weight]], na.rm = TRUE),
                unweighted_person_years = sum(time_at_risk_years, na.rm = TRUE),
                person_years = sum(time_at_risk_years_weighted, na.rm = TRUE),
                .groups = 'drop') %>%
      mutate(crude_rate_100000_person_years = 100000 * (count / person_years)) %>%
      rename(agegroup = all_of(age_grp)) %>%
      sdf_coalesce(1) %>%
      collect() %>%
      left_join(ESP_data, by =  "agegroup")%>%
      mutate(variance = (((crude_rate_100000_person_years^2) / count) * (ESP^2)),
             stand_rate = crude_rate_100000_person_years * ESP)
    
#    if (sum(is.na(rates_by_age$ESP)) != 0) {
#
#      stop("Missing population weights. Check the labels of the age groups")
#
#    }

    asmrs <- rates_by_age %>%
          group_by(across(all_of(!!group_vars))) %>%
          summarise(across(.cols = all_of(!!c("unweighted_count",
                                              "count",
                                              "sample",
                                              "pops",
                                              "stand_rate",
                                              "variance",
                                              "unweighted_person_years",
                                              "person_years")),
                           .fns = ~ sum(.x, na.rm = TRUE)),
                    .groups = 'drop') %>%
          mutate(count = as.integer(round(count, 0)),
                 variance = (variance / (total_esp^2)),
                 SE = sqrt(variance),
                 rate = stand_rate / total_esp) %>%
          left_join(poisson_data, by = c("count")) %>%
          mutate(lower = if_else(count < 100,
                                 ((((L * count) - count) * ((variance/count)^0.5)) + rate),
                                 rate - (1.96 * SE)),
                 upper = if_else(count < 100,
                                 ((((U * count) - count) * ((variance/count)^0.5)) + rate),
                                 rate + (1.96 * SE))) %>%
    mutate(age_standardised_rate_per_100000_person_years = rate,
           age_standardised_rate_lower_ci = lower,
           age_standardised_rate_upper_ci = upper) %>%
    mutate(across(.cols = all_of(c("count", "person_years")),
                  .fns = ~ as.integer(round(., 0)))) %>%
    mutate(crude_rate = map2_dbl(count, person_years, ~poisson.test(.x, .y)$estimate),
           crude_rate_per_100000_person_years = 100000 * crude_rate,
           crude_rate_lower_ci = map2_dbl(count, person_years, ~poisson.test(.x, .y)$conf.int[1]),
           crude_rate_lower_ci_per_100000_person_years = 100000 * crude_rate_lower_ci,
           crude_rate_upper_ci = map2_dbl(count, person_years, ~poisson.test(.x, .y)$conf.int[2]),
           crude_rate_upper_ci_per_100000_person_years = 100000 * crude_rate_upper_ci) %>%
    select(all_of(!!c(group_vars,
                      "unweighted_count", "count",
                      "sample", "pops",
                      "unweighted_person_years", "person_years",
                      "crude_rate_per_100000_person_years",
                      "crude_rate_lower_ci_per_100000_person_years",
                      "crude_rate_upper_ci_per_100000_person_years",
                      "age_standardised_rate_per_100000_person_years",
                      "age_standardised_rate_lower_ci",
                      "age_standardised_rate_upper_ci"))) %>%
    ungroup()

    asmrs

    
  } else if (type_of_outcome == "count") {
    
    grp_vars = c(age_grp, group_vars)
     
    rates_by_age <- data %>%
      mutate(outcome_weighted = .data[[outcome]] * .data[[weight]]) %>%
      group_by(across(all_of(!!grp_vars))) %>%
      summarise(unweighted_count = sum(.data[[outcome]], na.rm = TRUE),
                count = sum(outcome_weighted, na.rm = TRUE),
                sample = n(),
                pops = sum(.data[[weight]], na.rm = TRUE),
                .groups = 'drop') %>%
      mutate(crude_rate = 100 * (count / pops)) %>%
             rename(agegroup = all_of(age_grp)) %>%
      sdf_coalesce(1) %>%
      collect() %>%
      left_join(ESP_data, by = "agegroup") %>%
      mutate(variance = (((crude_rate^2) / count) * (ESP^2)),
             stand_rate = crude_rate * ESP)
 
#    if (sum(is.na(rates_by_age$ESP)) != 0) {
#      
#      stop("Missing population weights. Check the labels of the age groups")
#      
#    }

    asmrs <- rates_by_age %>%
      group_by(across(all_of(!!group_vars))) %>%
      summarise(across(.cols = all_of(!!c("unweighted_count",
                                          "count",
                                          "sample",
                                          "pops",
                                          "stand_rate",
                                          "variance")),
                       .fns = ~ sum(.x, na.rm = TRUE)),
                .groups = 'drop') %>%
      mutate(count = as.integer(round(count, 0)),,
             variance = (variance / (total_esp^2)),
             SE = sqrt(variance),
             rate = stand_rate / total_esp) %>%
      left_join(poisson_data, by = c("count")) %>%
      mutate(lower = if_else(count < 100,
                             ((((L * count) - count) * ((variance / count)^0.5)) + rate),
                             rate -(1.96 * SE)),
             upper = if_else(count < 100,
                             ((((U * count) - count) * ((variance / count)^0.5)) + rate),
                             rate + (1.96 * SE))) %>%
      mutate(age_standardised_rate = rate,
             age_standardised_rate_lower_ci = lower,
             age_standardised_rate_upper_ci = upper) %>%
      mutate(across(.cols = all_of(c("count", "pops")),
                    .fns = ~ as.integer(round(., 0)))) %>%
      mutate(crude_rate = map2_dbl(count, pops, ~poisson.test(.x, .y)$estimate),
             crude_rate = 100 * crude_rate,
             crude_rate_lower_ci = map2_dbl(count, pops, ~poisson.test(.x, .y)$conf.int[1]),
             crude_rate_lower_ci = 100 * crude_rate_lower_ci,
             crude_rate_upper_ci = map2_dbl(count, pops, ~poisson.test(.x, .y)$conf.int[2]),
             crude_rate_upper_ci = 100 * crude_rate_upper_ci) %>%
    select(all_of(!!c(group_vars,
                      "unweighted_count", "count", "sample", "pops",
                      "crude_rate", "crude_rate_lower_ci", "crude_rate_upper_ci",
                      "age_standardised_rate",
                      "age_standardised_rate_lower_ci",
                      "age_standardised_rate_upper_ci")))%>%
    ungroup()

    
    asmrs
    
  }
  
}

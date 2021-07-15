# consider how factors' influence different in different contexts
# (at open time/close time)

library(RMySQL)
library(yaml)
library(plyr)
library(pROC)
library("rjson")
library("stringr")
library(dplyr)
library(lmerTest)

change_factors = c('commits_on_files_touched_change', 'files_changed_change', 'num_commits_change', 'src_churn_change', 'test_churn_change', 'test_inclusion_change')

# define some processing functions
preprocessing<-function(v, colname) {
    if (colname %in% change_factors) {
      a = log(v + abs(min(v)) + 0.5)
      a = scale(a)
      a
    } else {
      a <- log(v + 0.5) # log the value
      a <- scale(a) # centering mean=0, variance=1
      a
    }
}
convertData_string <- function(df_project) {
    df_project[df_project == 'success'] = 1
    df_project[df_project == 'failure'] = 0
    df_project[df_project == 'male'] = 1
    df_project[df_project == 'female'] = 0

    df_project
}

convertData_preprocess <- function(df_project, exclude_cols = c()) {
  for (coln in colnames(df_project)) {
    if ((!coln %in% categorical_factors) & (!coln %in% exclude_cols)) {
        df_project[[coln]] = preprocessing(df_project[[coln]], coln)
    }
  }
  df_project
}

convertData_type <- function(df_project) {
  for (coln in colnames(df_project)) {
    if (coln %in% categorical_factors) {
      df_project[[coln]] = as.factor(df_project[[coln]]) # do not convert to logical factor
    }
  }
  df_project
}

# read data from mysql database
dbConfig <- yaml.load_file('R/config.yaml')

db_user <- dbConfig$mysql$user
db_password <- dbConfig$mysql$passwd
db_name <- dbConfig$mysql$db
db_host <- dbConfig$mysql$host # for local access
db_port <- as.numeric(dbConfig$mysql$port)

conn <- dbConnect(MySQL(), user = db_user, password = db_password,
                dbname = db_name, host = db_host, port = db_port)

q = "select * from new_pullreq_ese"
rs <- dbSendQuery(conn, q)
df <- fetch(rs, n = -1)

----------------------------- at open time ------------------------------

# keep factors at open time
keep_factors = c('followers', 'prev_pullreqs', 'integrator_availability', 'open_pr_num', 'project_age', 'team_size', 'description_length', 'core_member', 'ci_exists', 'friday_effect', 'contrib_gender', 'asserts_per_kloc', 'sloc', 'commits_on_files_touched_open', 'files_changed_open', 'num_commits_open', 'src_churn_open', 'test_churn_open', 'test_inclusion_open')

categorical_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_open", "contrib_gender")
binary_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_open", "contrib_gender")

selected_df = df[c(keep_factors, "lifetime_minutes", "project_id")]
selected_df = na.omit(selected_df)

# select real keep factors
selected_df = selected_df[c(keep_factors, "lifetime_minutes", "project_id")]

# preprocess & type convert
selected_df = convertData_string(selected_df)
selected_df = convertData_preprocess(selected_df, exclude_cols = c("project_id"))
selected_df = convertData_type(selected_df)

# how many columns left
colns = colnames(selected_df)

print(paste("selected_df rows: ", nrow(selected_df), sep=""))


#### build models
formula = paste("lifetime_minutes~", paste(keep_factors, collapse="+"), sep="")
formula = paste(formula, "+(1|project_id)", sep="")
print(formula)

dataset = "ese_results/models"

model_path = paste("Python/", dataset, "/context_process_open", ".RData", sep="")

selected_df$core_member = relevel(selected_df$core_member, ref = '0')
selected_df$contrib_gender = relevel(selected_df$contrib_gender, ref = '0')
selected_df$ci_exists = relevel(selected_df$ci_exists, ref = '0')
selected_df$friday_effect = relevel(selected_df$friday_effect, ref = '0')
selected_df$test_inclusion_open = relevel(selected_df$test_inclusion_open, ref = '0')

model_context_process <- lmer(
    formula = formula,
    verbose=TRUE,
    data=selected_df
)
vifs = car::vif(model_context_process)
print(vifs)

# calculate R-square
library(MuMIn)
Rs = as.data.frame(r.squaredGLMM(model_context_process))
print(Rs)

save(model_context_process, file=model_path)
print("finish build up model for process context at open time")

#----------------------- at close time ----------------------------

# keep factors at close time
keep_factors = c('followers', 'prev_pullreqs', 'integrator_availability', 'open_pr_num', 'project_age', 'team_size', 'description_length', 'core_member', 'ci_exists', 'friday_effect', 'contrib_gender', 'asserts_per_kloc', 'sloc', 'commits_on_files_touched_close', 'files_changed_close', 'num_commits_close', 'src_churn_close', 'test_churn_close', 'test_inclusion_close')

categorical_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_close", "contrib_gender")
binary_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_close", "contrib_gender")

selected_df = df[c(keep_factors, "lifetime_minutes", "project_id")]
selected_df = na.omit(selected_df)

# select real keep factors
selected_df = selected_df[c(keep_factors, "lifetime_minutes", "project_id")]

# preprocess & type convert
selected_df = convertData_string(selected_df)
selected_df = convertData_preprocess(selected_df, exclude_cols = c("project_id"))
selected_df = convertData_type(selected_df)

# how many columns left
colns = colnames(selected_df)

print(paste("selected_df rows: ", nrow(selected_df), sep=""))


#### build models
formula = paste("lifetime_minutes~", paste(keep_factors, collapse="+"), sep="")
formula = paste(formula, "+(1|project_id)", sep="")
print(formula)

dataset = "ese_results/models"

model_path = paste("Python/", dataset, "/context_process_close", ".RData", sep="")

selected_df$core_member = relevel(selected_df$core_member, ref = '0')
selected_df$contrib_gender = relevel(selected_df$contrib_gender, ref = '0')
selected_df$ci_exists = relevel(selected_df$ci_exists, ref = '0')
selected_df$friday_effect = relevel(selected_df$friday_effect, ref = '0')
selected_df$test_inclusion_close = relevel(selected_df$test_inclusion_close, ref = '0')

model_context_process <- lmer(
    formula = formula,
    verbose=TRUE,
    data=selected_df
)
vifs = car::vif(model_context_process)
print(vifs)

# calculate R-square
library(MuMIn)
Rs = as.data.frame(r.squaredGLMM(model_context_process))
print(Rs)

save(model_context_process, file=model_path)
print("finish build up model for process context at close time")


#----------------------- for change -----------------------------------

# keep factors for change
keep_factors = c('followers', 'prev_pullreqs', 'integrator_availability', 'open_pr_num', 'project_age', 'team_size', 'description_length', 'core_member', 'ci_exists', 'friday_effect', 'contrib_gender', 'asserts_per_kloc', 'sloc', 'commits_on_files_touched_change', 'files_changed_change', 'num_commits_change', 'src_churn_change', 'test_churn_change', 'test_inclusion_change')

categorical_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_change", "contrib_gender")
binary_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_change", "contrib_gender")

selected_df = df[c(keep_factors, "lifetime_minutes", "project_id")]
selected_df = na.omit(selected_df)

# select real keep factors
selected_df = selected_df[c(keep_factors, "lifetime_minutes", "project_id")]

selected_df$test_inclusion_change[selected_df$test_inclusion_change==2] = 0 # value 2 is not change, with the condition: open is 1 and close is 1; while value 1 refer to condition: open is 0 and close is 1

# preprocess & type convert
selected_df = convertData_string(selected_df)
selected_df = convertData_preprocess(selected_df, exclude_cols = c("project_id"))
selected_df = convertData_type(selected_df)

# how many columns left
colns = colnames(selected_df)

print(paste("selected_df rows: ", nrow(selected_df), sep=""))


#### build models
formula = paste("lifetime_minutes~", paste(keep_factors, collapse="+"), sep="")
formula = paste(formula, "+(1|project_id)", sep="")
print(formula)

dataset = "ese_results/models"

model_path = paste("Python/", dataset, "/context_process_change", ".RData", sep="")

selected_df$core_member = relevel(selected_df$core_member, ref = '0')
selected_df$contrib_gender = relevel(selected_df$contrib_gender, ref = '0')
selected_df$ci_exists = relevel(selected_df$ci_exists, ref = '0')
selected_df$friday_effect = relevel(selected_df$friday_effect, ref = '0')
selected_df$test_inclusion_change = relevel(selected_df$test_inclusion_change, ref = '0')

model_context_process <- lmer(
    formula = formula,
    verbose=TRUE,
    data=selected_df
)
vifs = car::vif(model_context_process)
print(vifs)

# calculate R-square
library(MuMIn)
Rs = as.data.frame(r.squaredGLMM(model_context_process))
print(Rs)

save(model_context_process, file=model_path)
print("finish build up model for process context for change condition")
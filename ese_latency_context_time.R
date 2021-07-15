# build for different time periods

library(RMySQL)
library(yaml)
library(plyr)
library(pROC)
library("rjson")
library("stringr")
library(dplyr)
library(lmerTest)

mix_effect = "project_id"

# remove num_comments VIF>5
keep_factors = c('followers', 'prev_pullreqs', 'integrator_availability', 'open_pr_num', 'project_age', 'team_size', 'description_length', 'num_code_comments', 'commits_on_files_touched_close', 'files_changed_close', 'num_commits_close', 'src_churn_close', 'test_churn_close', 'core_member', 'ci_exists', 'friday_effect', 'hash_tag', 'test_inclusion_close', 'same_user', 'has_comments', 'contrib_gender', 'prior_review_num', 'sloc', 'test_lines_per_kloc', 'reopen_or_not')

categorical_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_close", "contrib_gender", "hash_tag", "same_user", "has_comments", "reopen_or_not")
binary_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_close", "contrib_gender", "hash_tag", "same_user", "has_comments", "reopen_or_not")

# define some processing functions
preprocessing<-function(v) {
    a <- log(v + 0.5) # log the value
    a <- scale(a) # centering mean=0, variance=1
    a
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
        df_project[[coln]] = preprocessing(df_project[[coln]])
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

selected_df = df[c(keep_factors, "lifetime_minutes", "project_id", "last_close_time")]
selected_df = na.omit(selected_df)

train_model_func <- function(selected_df, which_model) {
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

    model_path = paste("Python/", dataset, "/", which_model, ".RData", sep="")

    selected_df$core_member = relevel(selected_df$core_member, ref = '0')
    selected_df$contrib_gender = relevel(selected_df$contrib_gender, ref = '0')
    selected_df$ci_exists = relevel(selected_df$ci_exists, ref = '0')
    selected_df$friday_effect = relevel(selected_df$friday_effect, ref = '0')
    selected_df$test_inclusion_close = relevel(selected_df$test_inclusion_close, ref = '0')
    selected_df$hash_tag = relevel(selected_df$hash_tag, ref = '0')
    selected_df$same_user = relevel(selected_df$same_user, ref = '1')
    selected_df$has_comments = relevel(selected_df$has_comments, ref = '0')
    selected_df$reopen_or_not = relevel(selected_df$reopen_or_not, ref = '0')

    model_context_time <- lmer(
        formula = formula,
        verbose=TRUE,
        data=selected_df
    )
    vifs = car::vif(model_context_time)
    print(vifs)

    # calculate R-square
    library(MuMIn)
    Rs = as.data.frame(r.squaredGLMM(model_context_time))
    print(Rs)

    save(model_context_time, file=model_path)
    print(paste("finish build up model for: ", which_model, sep=""))
}


# 1. first period (before 2016.6)
first_period_df = selected_df[selected_df$last_close_time<='2016-06-01 00:00:00',]

# 2. second period (2016.6-2018.6)
second_period_df = selected_df[(selected_df$last_close_time>'2016-06-01 00:00:00') & (selected_df$last_close_time<='2018-06-01 00:00:00'),]

# 3. third period (after 2018.6)
third_period_df = selected_df[selected_df$last_close_time>'2018-06-01 00:00:00',]

# find projects with >=1 pull requests in each period
first_period_project_ids = unique(first_period_df$project_id)
second_period_project_ids = unique(second_period_df$project_id)
third_period_project_ids = unique(third_period_df$project_id)
selected_project_ids = intersect(intersect(first_period_project_ids, second_period_project_ids), third_period_project_ids)

first_period_df = first_period_df[first_period_df$project_id %in% selected_project_ids,]
second_period_df = second_period_df[second_period_df$project_id %in% selected_project_ids,]
third_period_df = third_period_df[third_period_df$project_id %in% selected_project_ids,]


train_model_func(first_period_df, "context_time_first_period")
train_model_func(second_period_df, "context_time_second_period")
train_model_func(third_period_df, "context_time_third_period")
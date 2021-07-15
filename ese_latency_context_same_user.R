# build for pull request submitted and integrated by same and different users 

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
# remove prior_review_num VIF>5 when same_user=1
keep_factors = c('followers', 'prev_pullreqs', 'integrator_availability', 'open_pr_num', 'project_age', 'team_size', 'description_length', 'num_code_comments', 'commits_on_files_touched_close', 'files_changed_close', 'num_commits_close', 'src_churn_close', 'test_churn_close', 'core_member', 'ci_exists', 'friday_effect', 'hash_tag', 'test_inclusion_close', 'has_comments', 'contrib_gender', 'sloc', 'test_lines_per_kloc', 'reopen_or_not', "asserts_per_kloc", "comment_conflict")

categorical_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_close", "contrib_gender", "hash_tag", "has_comments", "reopen_or_not", "comment_conflict")
binary_factors = c("core_member", "ci_exists", "friday_effect", "test_inclusion_close", "contrib_gender", "hash_tag", "has_comments", "reopen_or_not", "comment_conflict")

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

selected_df = df[c(keep_factors, "lifetime_minutes", "project_id", "same_user")]
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
    selected_df$has_comments = relevel(selected_df$has_comments, ref = '0')
    selected_df$reopen_or_not = relevel(selected_df$reopen_or_not, ref = '0')
    selected_df$comment_conflict = relevel(selected_df$comment_conflict, ref = '0')

    model_context_same_user <- lmer(
        formula = formula,
        verbose=TRUE,
        data=selected_df
    )
    vifs = car::vif(model_context_same_user)
    print(vifs)

    # calculate R-square
    library(MuMIn)
    Rs = as.data.frame(r.squaredGLMM(model_context_same_user))
    print(Rs)

    save(model_context_same_user, file=model_path)
    print(paste("finish build up model for: ", which_model, sep=""))
}


# 1. same_user
same_user_df = selected_df[selected_df$same_user==1,]

# 2. diff_users
diff_users_df = selected_df[selected_df$same_user==0,]


train_model_func(same_user_df, "context_same_user")
train_model_func(diff_users_df, "context_diff_users")

# select factors from the strong correlations

import pandas as pd
import math
import copy
import sys


'''
extract the strongly correlated factors
params:
    1. spearman_df: the data frame of spearman correlation
    2. cramerv_df: the data frame of cramer'V values
    3. df_df: the data frame of 'degree of freedom' of categorical factors
    4. eta2_df: the data frame of eta partial square values
return: strong correlation dict (sc_dict) ({})
    key: factor name (str)
    value: strongly correlated factor list ([])
'''
def extract_sc_dict(spearman_df, cramerv_df, df_df, eta2_df):
    sc_dict = {} # the return variable

    # iterate spearman_df
    colnames = spearman_df.columns.values.tolist()
    for i in range(len(colnames)):
        colname_i = colnames[i]
        sc_dict.setdefault(colname_i, [])
        for j in range(len(colnames)):
            colname_j = colnames[j]
            if colname_i == colname_j:
                continue
            if abs(spearman_df[colname_i][colname_j]) > 0.7:
                sc_dict[colname_i].append(colname_j)

    # iterate cramerv_df
    colnames = cramerv_df.columns.values.tolist()
    for i in range(len(colnames)):
        colname_i = colnames[i]
        sc_dict.setdefault(colname_i, [])
        for j in range(len(colnames)):
            colname_j = colnames[j]
            if colname_i == colname_j:
                continue
            if abs(cramerv_df[colname_i][colname_j]) > 0.5 / (math.sqrt((df_df['df'][colname_i] - 1) * (df_df['df'][colname_j] - 1))):
                sc_dict[colname_i].append(colname_j)

    # iterate eta2_df
    colnames = eta2_df.columns.values.tolist()
    rownames = eta2_df.index.values.tolist()
    for i in range(len(rownames)):
        rowname_i = rownames[i]
        sc_dict.setdefault(rowname_i, [])
        for j in range(len(colnames)):
            colname_j = colnames[j]
            if rowname_i == colname_j:
                continue
            if abs(eta2_df[colname_j][rowname_i]) > 0.14:
                sc_dict[rowname_i].append(colname_j)
                sc_dict.setdefault(colname_j, [])
                sc_dict[colname_j].append(rowname_i)

    return sc_dict


# read the correlation results calculated before
spearman_df = pd.read_csv("correlations/continuous_correlation.csv", index_col=0) # spearman correlation result
cramerv_df = pd.read_csv("correlations/categorical_correlation.csv", index_col=0) # cramer's V result
df_df = pd.read_csv("correlations/categorical_correlation_df.csv", index_col=0) # degree freedom (df) of each categorical factor
eta2_df = pd.read_csv("correlations/cross_correlation.csv", index_col=0) # eta partial square result
sc_dict = extract_sc_dict(spearman_df, cramerv_df, df_df, eta2_df) # construct the dict for strong correlations


'''
preprocess the sc_dict for different situations, namely pull request submission and pull request close
'''
# 1. for submission
all_factors_submission = [
    "first_pr", "core_member", "contrib_gender", "contrib_affiliation", "social_strength", "prev_pullreqs", "followers", "sloc", "team_size", "project_age", "open_pr_num", "integrator_availability", "test_lines_per_kloc", "test_cases_per_kloc", "asserts_per_kloc", "perc_external_contribs", "requester_succ_rate", "bug_fix", "description_length", "ci_exists", "friday_effect", "num_commits_open", "src_churn_open", "files_changed_open", "commits_on_files_touched_open", "churn_addition_open", "churn_deletion_open", "test_churn_open", "test_inclusion_open"
]

'''
Some factors in sc_dict are for the close time,
so we need to form a new sc_dict only for submission time.
'''
sc_dict_submission = copy.deepcopy(sc_dict)
sc_dict_keys_submission = sorted(sc_dict_submission, key=lambda k:len(sc_dict_submission[k]), reverse=True)
for key in sc_dict_keys_submission:
    if key not in all_factors_submission:
        # remove all the values equal to key
        for k, v in sc_dict_submission.items():
            if key in v:
                v.remove(key)
        # remove the key
        sc_dict_submission.pop(key)

# ----------------------------------------------------------

# 2. for close time
all_factors_close = [
    "first_pr", "prior_review_num", "core_member", "contrib_gender", "contrib_affiliation", "inte_affiliation", "social_strength", "prev_pullreqs", "followers", "same_user", "sloc", "team_size", "project_age", "open_pr_num", "integrator_availability", "test_lines_per_kloc", "test_cases_per_kloc", "asserts_per_kloc", "perc_external_contribs", "requester_succ_rate", "bug_fix", "description_length", "hash_tag", "num_participants", "ci_exists", "part_num_code", "num_code_comments", "reopen_or_not", "friday_effect", "has_comments", "num_comments", "num_comments_con", "at_tag", "num_code_comments_con", "comment_conflict", "num_commits_close", "src_churn_close", "files_changed_close", "commits_on_files_touched_close", "churn_addition_close", "churn_deletion_close", "test_churn_close", "test_inclusion_close"
]
'''
Some factors in sc_dict are for the submission time,
so we need to form a new sc_dict only for close time.
'''
sc_dict_close = copy.deepcopy(sc_dict)
sc_dict_keys_close = sorted(sc_dict_close, key=lambda k:len(sc_dict_close[k]), reverse=True)
for key in sc_dict_keys_close:
    if key not in all_factors_close:
        # remove all the values equal to key
        for k, v in sc_dict_close.items():
            if key in v:
                v.remove(key)
        # remove the key
        sc_dict_close.pop(key)



def rmStrongCorrsByCount(sc_dict):

    sc_dict_copy = copy.deepcopy(sc_dict)

    # how many times the factors have been considered in previous studies (same_user & has_comments are added factors, and we want to keep these factors. Therefore, we set them with big values)
    fo_dict = {
        "contrib_affiliation": 2, "contrib_gender": 1, "core_member": 7, "first_pr": 1, "first_response_time": 2, "followers": 2, "inte_affiliation": 2, "prev_pullreqs": 3, "prior_review_num": 1, "same_affiliation": 1, "social_strength": 2, "asserts_per_kloc": 1, "integrator_availability": 2, "open_pr_num": 3, "perc_external_contribs": 1, "project_age": 2, "requester_succ_rate": 2, "sloc": 1, "team_size": 3, "test_cases_per_kloc": 1, "test_lines_per_kloc": 1, "at_tag": 2, "ci_exists": 3, "ci_latency": 2, "ci_test_passed": 2, "comment_conflict": 1, "description_length": 2, "friday_effect": 2, "hash_tag": 3, "num_code_comments": 1, "num_code_comments_con": 1, "num_comments": 4, "num_comments_con": 1, "num_participants": 2, "part_num_code": 1, "reopen_or_not": 1, "bug_fix": 1, "churn_addition_open": 2, "churn_deletion_open": 2, "commits_on_files_touched_open": 3, "files_changed_open": 2, "num_commits_open": 4, "src_churn_open": 3, "test_churn_open": 1, "test_inclusion_open": 2, "churn_addition_close": 2, "churn_deletion_close": 2, "commits_on_files_touched_close": 3, "files_changed_close": 2, "num_commits_close": 4, "src_churn_close": 3, "test_churn_close": 1, "test_inclusion_close": 2, "same_user": sys.maxsize, "has_comments": sys.maxsize
    }

    # some factors do not have strong correlations, we keep these factors by firstly remove them from the fo_dict
    count_keys = list(fo_dict.keys())
    correlation_keys = list(sc_dict.keys())
    rm_keys = list(set(count_keys) - set(correlation_keys))
    for key in rm_keys:
        fo_dict.pop(key)

    # then lets remove strong correlated factors
    removed_set_names = []
    removed_set_degrees = []
    kept_set = []

    # automatically and manually decide which factor to remove and which factor to keep
    
    while(True):
        count_keys = sorted(fo_dict, key=lambda k:fo_dict[k], reverse=False) # remove less popular factors first
        factor_A = count_keys[0]

        sc_dict_keys = sorted(sc_dict, key=lambda k:len(sc_dict[k]), reverse=True) # sort in dependency descending order (if the factor is strongly correlated too many factors, we cannot keep it)
        if len(sc_dict[sc_dict_keys[0]]) == 0: # no other strong correlation left, keep all the factors
            kept_set = sc_dict_keys
            break

        if len(sc_dict[factor_A]) == 0:
            fo_dict.pop(factor_A)
            continue # this factor does not have any strongly correlated factors
        
        same_count_factors = [factor_A]
        set_B = sc_dict[factor_A]
        for strong_corr in set_B:
            if fo_dict[strong_corr] == fo_dict[factor_A]:
                same_count_factors.append(strong_corr)
                
    
        if len(same_count_factors) == 1:
            choice = same_count_factors[0] # add factor_A into the removed factor set
        else:
            set_C = []
            max_degree = 0
            for f in same_count_factors:
                if len(sc_dict[f]) > max_degree:
                    set_C = [f]
                    max_degree = len(sc_dict[f])
                elif len(sc_dict[f]) == max_degree:
                    set_C.append(f)
            if len(set_C) == 1:
                choice = set_C[0]
            else:
                # manually decide to remove factor_D among set_C
                print("Please select one to remove - same degree factors: %s, degree: %s" % (",".join(set_C), max_degree))
                choice = input() # here the choice is factor_D

        removed_set_names.append(choice)
        removed_set_degrees.append(len(sc_dict[choice]))

        # update fo_dict and so_dict
        for corr_key in sc_dict[choice]:
            sc_dict[corr_key].remove(choice)
        sc_dict.pop(choice)
        fo_dict.pop(choice)
    
    # update kept_set and removed_set
    for factor in removed_set_names:
        if len(set(sc_dict_copy[factor]) & set(kept_set)) == 0:
            kept_set.append(factor)
            removed_set_names.remove(factor)

    print(removed_set_names)
    print(kept_set)
    print("\n\n")

    return removed_set_names, removed_set_degrees, kept_set


print("select the factors at pull request submission time")
removed_set_names_submission, removed_set_degrees_submission, kept_set_submission = rmStrongCorrsByCount(sc_dict_submission)

print("select the factors at pull request close time")
removed_set_names_close, removed_set_degrees_close, kept_set_close = rmStrongCorrsByCount(sc_dict_close)

print("finish")
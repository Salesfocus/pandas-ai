# Commit added traindocs and trainQueries
# should be save commit
git format-patch -1 d3a67f5f04a775674e45b8b810c2824555110edc --stdout > 1_added_traindocs_and_trainqueries.patch

# Added pgcache.py
# Notice: Some files for this commint may not exists in the current branch, so you may need to switch to the branch where this commit is located before running the command. 
git format-patch -1 0663b1441b973ae45b57ab5f37e938b1b2e5b1ed --stdout > 2_added_pgcache.patch

# refactor pgcache.py
# should be safe to apply this patch to the current branch, but you may want to check the changes before applying it.
git format-patch -1 74b2950968b92676c9721fee6cd85a83ba01646d --stdout > 3_refactor_pgcache.patch

#riase exception if groupby_not_found
# should be safe to apply this patch to the current branch, but you may want to check
git format-patch -1 de02c42fb2f0e1272547f8a31e7ab4dfcddb739e --stdout > 4_raise_exception_if_groupby_not_found.patch


#add logging to baseagent
# should be safe to apply this patch to the current branch, but you may want to check
git format-patch -1 34a5a2a991e9ab9953a67c994cb1eee2a51c3c44 --stdout > 5_add_logging_to_baseagent.patch

# add enforce_grouping parameter to Agent and BaseAgent classes
# multiple files, so you may want to check the changes before applying the patch.
git format-patch -1 f41e1685e440e2087871c21e7396efcd768d9697 --stdout > 6_add_enforce_grouping_parameter.patch

# feat: add enforce_grouping parameter to chat method in BaseAgent class
# should be safe to apply this patch to the current branch, but you may want to check
git format-patch -1 50da2de2e37f448638505b5d942434cc991d6c8f --stdout > 7_add_enforce_grouping_parameter_to_chat_method.patch

# Fix Gemini - Google
# should be safe to apply this patch to the current branch, but you may want to check
git format-patch -1 551b84cd734496c1d74b68d9199111e5a3447423 --stdout > 8_fix_gemini_google.patch
git format-patch -1 b8576501f6db9bd07a44790f5259d135a13cc70f --stdout > 8_1_fix_gemini_google.patch

#add shells 
git format-patch -1 54f94cb91a904177796023055cdd2670174797a9 --stdout > 9_add_shells.patch


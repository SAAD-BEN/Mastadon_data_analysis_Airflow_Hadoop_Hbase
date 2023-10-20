#!/usr/bin/env python3
import sys

current_user = None
earliest_followers = float('inf')
earliest_time = "00:00:00"
engagement_rate_sum = 0.0
count = 1

for line in sys.stdin:
    user, data = line.strip().split('\t')
    followers = int(data.split('@')[1])
    time = data.split('@')[0]
    engagement_rate = float(data.split('@')[2])

    if current_user == user:
        if time < earliest_time:
            earliest_time = time
            earliest_followers = followers
        engagement_rate_sum += engagement_rate
        count += 1
    else:
        if current_user is not None:
            if count > 0:
                engagement_rate_avg = engagement_rate_sum / count
                print(f"{current_user}\tengagement_rate:{engagement_rate_avg:.4f}")
                print(f"{current_user}\tfollowers:{earliest_followers}")
        current_user = user
        earliest_followers = followers
        earliest_time = time
        engagement_rate_sum = engagement_rate
        count = 1

# Print the last user's data
if current_user is not None:
    if count > 0:
        engagement_rate_avg = engagement_rate_sum / count
        print(f"{current_user}\tfollowers:{earliest_followers}")
        print(f"{current_user}\tengagement_rate:{engagement_rate_avg:.4f}")

# """
# This script reads the output of the mapper.py script and reduces it to a list of users and their earliest follower count.
# The input to the reducer is sorted by user name, so all the records for a particular user are grouped together.
# The reducer then iterates through each record for a user and keeps track of the earliest follower count and the time it was recorded.
# The output of the reducer is a list of users and their earliest follower count.
# """
# #!/usr/bin/env python3
# import sys 

# current_user = None
# current_followers = 0
# current_time = "00:00:00"
# engagement_rate_sum = 0.0
# count = 1

# for line in sys.stdin:
# 	user, data = line.strip().split('\t')
# 	followers = data.split('@')[0] + '@' + data.split('@')[1]
# 	engagement_rate = data.split('@')[2]
# 	user = user.strip()
# 	followers = followers.strip()
# 	engagement_rate = engagement_rate.strip()
# 	if current_user == user:
# 		if current_time > followers.split('@')[0]:
# 			current_time = followers.split('@')[0]
# 			current_followers = int(followers.split('@')[1])
# 		engagement_rate_sum += float(engagement_rate)
# 		count += 1
# 	else:
# 		if current_user:
# 			# Calculate the average engagement rate
# 			engagement_rate_avg = engagement_rate_sum / count
# 			print(f"{current_user}\tfollowers:{current_followers}@engagement_rate:{engagement_rate_avg:.4f}")
# 		current_user = user
# 		current_followers = int(followers.split('@')[1])
# 		current_time = followers.split('@')[0]
# 		engagement_rate_sum = float(engagement_rate)
# 		count = 1
#!/usr/bin/env python3
import sys
# import happybase

# # Initialize a connection to HBase
# connection = happybase.Connection('localhost', 9090, autoconnect=False)
# # Select a table family name
# table_family = 'data'

# # Define the HBase tables
# user_table = connection.table('user_data')
# croissance_table = connection.table('croissance_data')
# language_table = connection.table('language_data')
# toot_with_media_table = connection.table('toot_with_media_data')
# emoji_table = connection.table('emoji_data')
# url_table = connection.table('website_data')

current_user = None
current_croissance = None
current_language = None
current_toot_with_media = None
current_emoji = None
current_url = None

earliest_followers = float('inf')
old_time = "00:00:00"

engagement_rate_sum = 0.0000
croissance_sum = 0
language_sum = 0
toot_with_media_sum = 0
emoji_sum = 0
url_sum = 0
count = 1

unique_users_roissance = []

user_dict = {}
croissance_dict = {}
language_dict = {}
toot_with_media_dict = {}
emoji_dict = {}
url_dict = {}

for line in sys.stdin:
    key = line.strip().split('\t')[0]
    key = key.strip()
    # if key starts with user, it's a user record
    if key.startswith("user"):
        user, strdata = line.strip().split('\t')
        # string to dict
        data = eval(strdata.strip())
        followers = int(data["followers"])
        time = data['date']
        engagement_rate = float(data["engagement_rate"])

        if current_user == user:
            if time > old_time:
                old_time = time
                earliest_followers = followers
            engagement_rate_sum += engagement_rate
            count += 1
        else:
            if current_user is not None:
                if count > 0:
                    engagement_rate_avg = engagement_rate_sum / count
                    user_dict["engagement_rate"] = engagement_rate_avg
                    user_dict["followers"] = earliest_followers
                    print(f"{current_user}\t{user_dict}")
                    # user_table.put(current_user.encode(), {table_family.encode(): str(user_dict).encode()})
            current_user = user
            earliest_followers = followers
            old_time = time
            engagement_rate_sum = engagement_rate
            count = 1

    # if key starts with croissance, it's a croissance record
    elif key.startswith("croissance"):
        croissance, strdata = line.strip().split('\t')
        data = eval(strdata.strip())
        user_id = data["user_id"]
        if croissance == current_croissance:
            if current_croissance is not None:
                if user_id not in unique_users_roissance:
                    unique_users_roissance.append(user_id)
                    croissance_sum += data["value"]
        elif croissance != current_croissance:
            if current_croissance is not None:
                croissance_dict["count"] = croissance_sum
                print(f"{current_croissance}\t{croissance_dict}")
            current_croissance = croissance
            croissance_sum = 1
            unique_users_roissance = [user_id]

    #if key starts with language, it's a language record
    elif key.startswith("language"):
        language, strdata = line.strip().split('\t')
        data = eval(strdata.strip())
        if language == current_language:
            if current_language is not None:
                language_sum += data["value"]
        elif language != current_language:
            if current_language is not None:
                language_dict["count"] = language_sum
                print(f"{current_language}\t{language_dict}")
            current_language = language
            language_sum = 1

    elif key.startswith("toot_with_media"):
        toot_with_media, strdata = line.strip().split('\t')
        data = eval(strdata.strip())
        if toot_with_media == current_toot_with_media:
            if current_toot_with_media is not None:
                toot_with_media_sum += data["value"]
        elif toot_with_media != current_toot_with_media:
            if current_toot_with_media is not None:
                toot_with_media_dict["count"] = toot_with_media_sum
                print(f"{current_toot_with_media}\t{toot_with_media_dict}")
            current_toot_with_media = toot_with_media
            toot_with_media_sum = 1
    
    elif key.startswith("emoji"):
        emoji_id , strdata = line.strip().split('\t')
        data = eval(strdata.strip())
        if emoji_id == current_emoji:
            if current_emoji is not None:
                emoji_sum += data["value"]
        elif emoji_id != current_emoji:
            if current_emoji is not None:
                emoji_dict["count"] = emoji_sum
                print(f"{current_emoji}\t{emoji_dict}")
            current_emoji = emoji_id
            emoji_sum = 1

    elif key.startswith("website"):
        website_id , strdata = line.strip().split('\t')
        data = eval(strdata.strip())
        if website_id == current_url:
            if current_url is not None:
                url_sum += data["value"]
        elif website_id != current_url:
            if current_url is not None:
                url_dict["count"] = url_sum
                print(f"{current_url}\t{url_dict}")
            current_url = website_id
            url_sum = 1
                

# Print the last data
if current_croissance is not None:
    croissance_dict["count"] = croissance_sum
    print(f"{current_croissance}\t{croissance_dict}")

if current_user is not None:
    if count > 0:
        engagement_rate_avg = engagement_rate_sum / count
        print(f"{current_user}\t{user_dict}")
        # user_table.put(current_user.encode(), {table_family.encode(): str(user_dict).encode()})

if current_language is not None:
    language_dict["count"] = language_sum
    print(f"{current_language}\t{language_dict}")

if current_toot_with_media is not None:
    toot_with_media_dict["count"] = toot_with_media_sum
    print(f"{current_toot_with_media}\t{toot_with_media_dict}")

if current_emoji is not None:
    emoji_dict["count"] = emoji_sum
    print(f"{current_emoji}\t{emoji_dict}")

if current_url is not None:
    url_dict["count"] = url_sum
    print(f"{current_url}\t{url_dict}")
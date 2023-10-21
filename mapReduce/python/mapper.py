#!/usr/bin/env python3
import json
import sys
import re
from urllib.parse import urlparse

def process_data(input_data):
    pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    
    for line in input_data:
        try:
            data = json.loads(line)
            toot_with_media_dict = {}
            toot_with_media_id = 'toot_with_media'
            
            created_at = data["created_at"].split(' ')[1].split('+')[0]
            account = data.get("account")
            media_attachments = data.get("media_attachments")
            emojis = data.get("emojis") if data.get("emojis") else []
            websites = data.get("content") if data.get("content") else []
            
            if account:
                user_id = 'user:' + str(account.get('id'))
                followers = int(account.get('followers_count', 0))
                reblogs_count = data.get('reblogs_count', 0)
                favourites_count = data.get('favourites_count', 0)

                engagement_rate = (reblogs_count + favourites_count) / followers if followers > 0 else 0

                croissance_id = "croissance:" + account.get('created_at').split('-')[0] + '-' + account.get('created_at').split('-')[1]

                user_data = {
                    "date": created_at,
                    "followers": followers,
                    "engagement_rate": engagement_rate
                }

                croissance_data = {"value": 1, "user_id": user_id}

                # Emit key-value pairs for the reducer
                print(f"{user_id}\t{user_data}")
                print(f"{croissance_id}\t{croissance_data}")
            
            if media_attachments:
                toot_with_media_dict["value"] = 1
                print(f"{toot_with_media_id}\t{toot_with_media_dict}")

            language_id = "language:" + data.get('language')
            language_data = {"value": 1}
            print(f"{language_id}\t{language_data}")

            if emojis != []:
                for emoji in emojis:
                    emoji_id = "emoji:" + emoji.get('shortcode')
                    emoji_data = {"value": 1}
                    print(f"{emoji_id}\t{emoji_data}")
            
            if websites != []:
                urls = re.search(pattern, websites)
                if urls:
                    website_id = "website:" + urlparse(urls.group(0)).netloc
                    website_data = {"value": 1}
                    print(f"{website_id}\t{website_data}")            

        except Exception as e:
            # Log exceptions to standard error
            print(f"Error: {str(e)}", file=sys.stderr)

# Example usage of the process_data function
if __name__ == "__main__":
    input_data = sys.stdin
    process_data(input_data)

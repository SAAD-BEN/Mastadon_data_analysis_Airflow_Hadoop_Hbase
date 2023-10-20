#!/usr/bin/env python3
import json
import sys

def process_data(input_data):
    for line in input_data:
        try:
            data = json.loads(line)
            
            created_at = data["created_at"].split(' ')[1].split('+')[0]
            account = data.get("account")
            
            if account:
                user_id = 'user:' + str(account.get('id'))
                followers = int(account.get('followers_count', 0))
                reblogs_count = data.get('reblogs_count', 0)
                favourites_count = data.get('favourites_count', 0)

                engagement_rate = (reblogs_count + favourites_count) / followers if followers > 0 else 0

                croissance_id = "Croissance:" + account.get('created_at').split(' ')[0]
                language_id = "Language:" + data.get('language')

                user_data = {
                    "date": created_at,
                    "followers": followers,
                    "engagement_rate": engagement_rate
                }

                croissance_data = {"value": 1, "user_id": user_id}
                language_data = {"value": 1}

                # Emit key-value pairs for the reducer
                print(f"{user_id}\t{user_data}")
                print(f"{croissance_id}\t{croissance_data}")
                print(f"{language_id}\t{language_data}")

        except Exception as e:
            # Log exceptions to standard error
            print(f"Error: {str(e)}", file=sys.stderr)

# Example usage of the process_data function
if __name__ == "__main__":
    input_data = sys.stdin
    process_data(input_data)

#!/usr/bin/env python3
import json
import sys

def process_data(input_data):
    for line in input_data:
        try:
            data = json.loads(line)
            date = data["created_at"].split(' ')[1].split('+')[0]
            account = data["account"]
            if account:
                user_id = 'user:' + str(account.get('id'))
                followers = int(account.get('followers_count', 0))
                reblogs_count = data['reblogs_count'] if 'reblogs_count' in data else 0
                favourites_count = data['favourites_count'] if 'favourites_count' in data else 0

                # Calculate the engagement rate
                engagement_rate = (reblogs_count + favourites_count) / followers if followers > 0 else 0

                # Emit key-value pair for the reducer
                print(f"{user_id}\t{date}@{followers}@{engagement_rate:.4f}")

        except Exception as e:
            # Add a print statement to capture and log any exceptions
            print("Error:", str(e), file=sys.stderr)

# Example usage of the process_data function
if __name__ == "__main__":
    input_data = sys.stdin
    process_data(input_data)
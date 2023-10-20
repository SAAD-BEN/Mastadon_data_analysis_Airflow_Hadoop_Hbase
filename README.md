# Mastadon data analysis Airflow Hadoop_Hbase

## Mastodon Raw Data Structure

This section provides an overview of the data structure used to represent Mastodon posts in their raw form. Mastodon is a federated social media platform, and this structure helps you understand how to organize and access information about individual posts within the Mastodon ecosystem.

### Post Data Structure

Each Mastodon post is represented as a dictionary with the following key-value pairs:

- `id`: The unique identifier for the post.
- `created_at`: The date and time when the post was created.
- `in_reply_to_id`: The ID of the post this post is replying to (if applicable).
- `in_reply_to_account_id`: The ID of the account being replied to (if applicable).
- `sensitive`: A Boolean indicating whether the post contains sensitive content.
- `spoiler_text`: Text used to provide a content warning for sensitive posts.
- `visibility`: The visibility of the post (e.g., public, unlisted, private).
- `language`: The language in which the post is written.
- `uri`: The URI of the post.
- `url`: The URL of the post.
- `replies_count`: The count of replies to the post.
- `reblogs_count`: The count of reblogs (shares) of the post.
- `favourites_count`: The count of times the post has been favorited.
- `edited_at`: Date and time when the post was edited (if applicable).
- `content`: The content of the post (the actual message or status).

### Account Data Structure

Each post includes information about the user who made the post. This information is structured as follows:

- `id`: The unique identifier for the user account.
- `username`: The username of the user.
- `acct`: The account identifier (username with domain).
- `display_name`: The display name of the user.
- `locked`: A Boolean indicating whether the user's account is locked.
- `bot`: A Boolean indicating whether the user is a bot.
- `discoverable`: A Boolean indicating whether the user's account is discoverable.
- `group`: A Boolean indicating whether the user's account is a group account.
- `created_at`: The date and time when the user account was created.
- `note`: A text note or bio associated with the user.
- `url`: The URL of the user's profile.
- `uri`: The URI of the user's profile.
- `avatar`: URL to the user's avatar.
- `avatar_static`: URL to the user's static avatar.
- `header`: URL to the user's header image.
- `header_static`: URL to the user's static header image.
- `followers_count`: The count of followers of the user.
- `following_count`: The count of accounts the user is following.
- `statuses_count`: The count of posts made by the user.
- `last_status_at`: The date and time of the user's last post.
- `emojis`: A list of custom emojis used by the user.
- `fields`: Additional information fields associated with the user.

### Additional Data

- `media_attachments`: A list of media attachments (e.g., images) included in the post.
- `mentions`: A list of mentions of other users in the post.
- `tags`: A list of tags associated with the post.
- `emojis`: A list of custom emojis used in the post.
- `card`: Additional card information (if applicable).
- `poll`: Poll information (if a poll is attached to the post).

This data structure helps you understand how Mastodon posts are organized and can be used as a reference when working with raw Mastodon data.

For more information about Mastodon, please refer to the official Mastodon documentation.

---

**Note:** This is a generalized data structure for Mastodon posts and user accounts. Actual data may vary depending on the instance and post content.

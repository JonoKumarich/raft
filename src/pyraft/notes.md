TODO: Ability to commit entries

When the leader gets the appendEntriesResponse, it will keep a tally of all successful replies for a given index number
When the tally passes the majority, it will set the value to commited?


# Bugs:

- Send message, stop leader, and send message to new elected leader crashed the program

# Questions:

Sending a message:
- What happens when a single message gets dropped: Do we resend that message to the server even when we have a majority?
    - Yes we keep sending until we get a response


- SO: We send everything past the match index for each sertver

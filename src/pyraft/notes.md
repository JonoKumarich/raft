TODO: Ability to commit entries

When the leader gets the appendEntriesResponse, it will keep a tally of all successful replies for a given index number
When the tally passes the majority, it will set the value to commited?


# Bugs:

- Send message, stop leader, and send message to new elected leader crashed the program

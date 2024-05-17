# Message Format:

When a sever recieves a message, it needs to check if it came form a server inside or outside of the network.


Commands:
    get / set / del / incr -> These are commands that will be sent form the client
    rpc AppendEntries {<data>} / rpc VoteRequest {<data>} -> Format that will be sent from a client


On initialization.
    - All n servers are started
    - Once all n servers are started, connect each server to each other

- SQlite should be used for persistence
- Pydantic can use for message serialization

# State machine
- We have a controller class
    - It contains a server
    - And a state machine
    - It listens for entries from the server and appends to a queue
    - When new entry recieved it then calls the appropriate handler function
    - The handler function interacts with the state machine itself

# Simple in-memory data storage with raft consensus implimentation 


## Archetecture - **Client** can query any node but the request will be redirected to the **Leader** only

- Each instance of server will contain following structural code 
    
    - At initialization every instance of the server will be a **Follower** 
    - Voting -> all node's when initiated would send a request to a voting server which will determine there role's
    - Currently the voting algorithm will randomly assign role's 

    ```rust 

    enum Role { 
        Follower , 
        Candidate , 
        Leader , 
    }
    ``` 

    - Whenever any **Node** recieves a request it redirects it to the **Leader** incase its not the leader itself 

    ```rust 
    if this.node != Role::Leader{
        rpc Propose(ProposeRequest) returns (ProposeResponse);

    }
    ```

    - If **Node** is **Leader** execute the command and **Broadcast** over all the nodes 

    ```rust  
    if this.node == Role::Leader{
            
        command_execute(body.command);

        Log.append(body.command);



            // there can be 2 ways of broadcasting , either broadcast to all the nodes and create a seperate function in all the nodes to catch the boardcast and update the entries / logs , or either broadcast for all logs except the one which sent us the request as that one will only recieve a success indicating compleaton and permission to replicate
            
            // found the earlier one more cleaner tho , just documenting my thought's  

        return success ;
    }
    ```

    - **log_replicate** is invoked using a broadcast from the **Leader Node** for updating the **Log** and State of the DataStore

    ```rust 

    #[route('/replicate')]
    log_replicate(content){
        // recieves the request from the leader and replicate its own state
            


    }
    ```

### Voting 
- Everyone is follower with a randomly assigned ElectionTimeout between 150–300 ms 
- Some/All (potentially) followers (if no Heartbeats till ElectionTimeout) become Candidate and increment the term and add one vote to themselve's 
a
 
    ```rust 
        role = Role::Candidate;
        current_term += 1;
        voted_for = Some(self.id);
        votes_received = 1;
   ```


- Issues `RequestVote RPCs` to all of the other Node's in the cluster 
    
    ```rust 
        struct RequestVote {
            term: u64,
            candidate_id: u64,
            last_log_index: u64,
            last_log_term: u64,  // if the log's are outdated raft wont let it be the leader 
        }
    ```

- Votes are to be given as fist-come-first-serve basis given the following criteria matches 
    - The candidate's term is at least as recent as its own.
    - It hasn't already voted for another candidate in that term.
    - The candidate's log is at least as up-to-date as its own.

- Candidate with majority vote's win (not more votes) and will start sending heartbeats (empty `ReplicateLog`)
- If nobody gets a majority, the election times out and another election starts with a new term.
- If any node discovers a higher term, it updates its term and steps down to Follower if necessary

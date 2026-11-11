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

# Simple in-memory data storage with raft consensus implimentation 


## Archetecture - **Client** can query any node but the request will be redirected to the **Leader** only

- Each instance of server will contain following structural code 
    ```rust 
        
        enum Role { 
            Follower , 
            Candidate , 
            Leader , 
        }
    
        // at the initialisation of every instance of server each server will be given a role 
        // one will be leader rest candidate at random ( for testing )

        if this.node != Role::Leader{
            this.node.leader::process_command(body); // via grpc
        }
        
        


    ```

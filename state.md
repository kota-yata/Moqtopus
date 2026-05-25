## State Machine Descriptions
Below are state machines implemented in Moqtopus.

Note: I have named class methods to be consistent to the state names in these diagrams as much as possible, but some may differ

### Session-level state
```mermaid
stateDiagram-v2
    [*] --> Init
    Init --> SetupInProgress : QUIC connection open
    SetupInProgress --> Ready : local SETUP sent + peer SETUP received
    Ready --> Draining : peer GoAway
    Ready --> Closing : local close / protocol_violation / transport_error
    Draining --> Closing : local close / protocol_violation / transport_error
    Closing --> Closed : shutdown_complete
    Closed --> [*]
    %% global error path
    Init --> Closing : protocol_violation / transport_error
    SetupInProgress --> Closing : protocol_violation / transport_error
```


### SUBSCRIBE request stream
```mermaid
stateDiagram-v2
    [*] --> Pending : SUBSCRIBE
    Pending --> Established : SUBSCRIBE_OK (track assigned)
    Pending --> Terminated : REQUEST_ERROR / stream fin / abort / terminate()
    Established --> UpdateFailed : REQUEST_ERROR
    Established --> Terminated : PUBLISH_DONE / stream fin / abort / terminate()
    UpdateFailed --> Terminated : cleanup / terminate()
    Terminated --> [*]
    %% owner-driven stop/error paths
    Pending --> Terminated : owner stop / protocol error
    Established --> Terminated : owner stop / protocol error
```

You see `REQUEST_ERROR` shows up between Established and UpdateFailed state, with no `REQUEST_UPDATE` in the diagram. Sending `REQUEST_UPDATE` requires the state to be Established, but the transmission itself does not cause a state transition. This is because multiple `REQUEST_UPDATE`s are allowed to be sent in parallel, and projecting each `REQUEST_UPDATE`'s state to the overall stream state will be super complex. Instead, each `REQUEST_UPDATE` is managed as `PendingRequest` type, waiting for either `REQUEST_OK` or `REQUEST_ERROR` to be returned.

package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Timer;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 75;

    // TODO: add fields for client timer ...
    private final PaxosRequest request;
}

// TODO: add more timers here ...

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_RETRY_MILLIS = 25;
}

@Data
final class HeartbeatCheckTimer implements Timer {
    static final int HEARTBEAT_CHECK_RETRY_MILLIS = 60;
}
@Data
@EqualsAndHashCode
final class P1aTimer implements Timer {
    static final int P1A_RETRY_MILLIS = 25;
    private final P1aMessage msg;

    P1aTimer(P1aMessage msg) {
        this.msg = msg;
    }
}

@Data
@EqualsAndHashCode
final class P2aTimer implements Timer {
    static final int P2A_RETRY_MILLIS = 25;
    private final P2aMessage msg;

    /*P2aTimer(P2aMessage msg) {
        this.msg = msg;
    }*/
}

@Data
@EqualsAndHashCode
final class ProposeTimer implements Timer {
    static final int PROPOSE_RETRY_MILLIS = 30;
    private final ProposalMsg msg;
    private Address to;
    ProposeTimer(ProposalMsg msg, Address a) {
        this.msg = msg;
        this.to = a;
    }
}

@Data
@EqualsAndHashCode
final class BatchP2aTimer implements Timer {
    static final int P2A_RETRY_MILLIS = 30;
    private final BatchP2aMessage msg;

   /* BatchP2aTimer(BatchP2aMessage msg) {
        this.msg = msg;
    }*/
}
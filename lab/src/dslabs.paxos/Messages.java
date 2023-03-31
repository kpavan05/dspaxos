package dslabs.paxos;


// TODO: add messages for PAXOS here ...

import dslabs.framework.Address;
import dslabs.framework.Message;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
class HeartbeatMsg implements Message {
    private final Address senderId;
    private final Integer minSlotOut;
    private final Ballot ballot_num;
    private final boolean isLeaderActive;
}


@Data
@EqualsAndHashCode
class LogStateMsg implements Message {
    private final Integer slotOut;
    private final Integer lastCleared;
}
@Data
@EqualsAndHashCode
class BatchDecisionMsg implements Message {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BatchDecisionMsg that = (BatchDecisionMsg) o;
        return missingDecisions.equals(that.missingDecisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(missingDecisions);
    }

    private final HashSet<DecisionMsg> missingDecisions;
}


@Data
@EqualsAndHashCode
class ProposalMsg implements Message {
    private final Address senderId;
    private final Integer slot_number;
    private final PaxosRequest request;
}

@Data
@EqualsAndHashCode
class AdoptedMsg implements Message {
    private final String senderId;
    private final Ballot ballot_number;
    private final HashMap<Integer,Pvalue> acceptedMap;
    @Override
    public String toString() {
        return "senderId:" + senderId + ", Ballot:" + ballot_number;
    }
}

@Data
@EqualsAndHashCode
class PreemptedMsg implements Message {
    private final String senderId;
    private final Ballot ballot_number;
}

@Data
class P1aMessage implements Message {
    private final String senderId;
    private final Ballot ballot_number;
    private final HashMap<Integer, PaxosRequest> decisions;
}

@Data
@EqualsAndHashCode
class P2aMessage implements Message {
    private final String senderId;
    private final Ballot ballot_number;
    private final Integer slot_number;
    private final PaxosRequest request;

}

@Data
@EqualsAndHashCode
class BatchP2aMessage implements Message {
  private final String senderId;
  private final Ballot ballot_number;
  private final HashSet<P2aMessage> p2aMessages;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BatchP2aMessage that = (BatchP2aMessage) o;
        return senderId.equals(that.senderId) &&
                ballot_number.equals(that.ballot_number);
    }

    @Override
    public int hashCode() {
        return Objects.hash(senderId, ballot_number);
    }
}
@Data
@EqualsAndHashCode
class BatchP2bMessage implements Message {
    private final String senderId;
    private final Ballot ballot_number;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BatchP2bMessage that = (BatchP2bMessage) o;
        return senderId.equals(that.senderId) &&
                ballot_number.equals(that.ballot_number) &&
                requestorId.equals(that.requestorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(senderId, ballot_number, requestorId);
    }

    private final HashSet<P2aMessage> p2aMessages;
    private final String requestorId;
}
@Data
@EqualsAndHashCode
class P1bMessage implements Message {
    //private final Address senderId;
    private final String senderId;
    private final Ballot ballot_number;
    private final HashMap<Integer, Pvalue> acceptedMap;
    private final HashMap<Integer, PaxosRequest> decisionMap;
    private final String requestorId;

    @Override
    public String toString() {
        return "senderId:" + senderId + ", Ballot:" + ballot_number + ", requestor:" + requestorId;
    }
}

@Data
@EqualsAndHashCode
class P2bMessage implements Message {
    //private final Address senderId;
    private final String senderId;
    private final Ballot ballot_number;
    private final Integer slot_number;
    private final String requestorId;
}

@Data
@EqualsAndHashCode
class DecisionMsg implements Message {
    private final String senderId;
    private final Integer slot_number;
    private final PaxosRequest request;
}
package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.java.Log;

enum MsgType {
    P1A,
    P2A,
    P1B,
    P2B,
    PROPOSAL,
    DECISION,
    BATCH_P2A,
    BATCH_P2B,
    BATCH_DECISION,
    HEART_BEAT;
};

@Data
class PaxosLogEntry implements Serializable, Comparable{
    private PaxosRequest request;
    private PaxosLogSlotStatus status;
    private int slotNumber;

    public PaxosLogEntry(PaxosRequest r, PaxosLogSlotStatus s, int idx) {
        request = r;
        status = s;
        slotNumber = idx;
    }

    @Override
    public int compareTo(Object o) {
        PaxosLogEntry other = (PaxosLogEntry) o;
        if (slotNumber < other.slotNumber) {
            return -1;
        } else {
            return 1;
        }
    }
}

@Log
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    static final int INITIAL_BALLOT_NUMBER = 0;
    static final int MIN_LOG = 20;
    private final Address[] servers;

    // TODO: declare fields for your implementation ...
    private AMOApplication app;

    private int minSlotOut;
    private int lastClearedSlot;
    private int hbCheckTimerTick;
    private boolean startHBCheck;
    private Map<Address, Integer> serverHBMap = new HashMap<>();

    private Map<Integer,PaxosLogEntry> replicatedLogMap = new HashMap<>();
    private Queue<PaxosRequest> requests = new LinkedList<>();
    private HashMap<Integer, PaxosRequest> decisions = new HashMap<>();
    private HashSet<PaxosRequest> reProposedSet = new HashSet<>();
    private HashMap<Address, Integer> requestProcessed = new HashMap<>();
    private Map<Address, Integer> followerSlotMap = new HashMap<>();

    private Replica replicaNode;
    private Leader leaderNode;
    private Acceptor acceptorNode;
    private Map<Integer, PaxosRequest> proposalSet = new HashMap<>();


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;

        // TODO: wrap app inside AMOApplication ...
        this.app = new AMOApplication<>(app);
    }


    @Override
    public void init() {
        // TODO: initialize fields ...
        hbCheckTimerTick = 0;
        followerSlotMap.clear();

        minSlotOut = -1;
        lastClearedSlot = 0;
        replicatedLogMap.clear();
        proposalSet.clear();
        startHBCheck = false;
        serverHBMap.clear();

        String aString = address().toString();
        char n = aString.charAt(aString.length()-1);
        replicaNode = new Replica(aString+ "-r:" + n);
        replicaNode.init();

        acceptorNode = new Acceptor(aString+ "-a:" + n);
        acceptorNode.init();

        leaderNode = new Leader(aString + "-l:" + n);
        leaderNode.init();
        set(new HeartbeatCheckTimer(), HeartbeatCheckTimer.HEARTBEAT_CHECK_RETRY_MILLIS);
    }


    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the servers's local log.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        PaxosLogEntry e = replicatedLogMap.get(logSlotNum);
        if (e != null) return e.status();

        return PaxosLogSlotStatus.EMPTY;
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log. If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. If
     * clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     */
    public Command command(int logSlotNum) {
        // Your code here...
        PaxosLogEntry e = replicatedLogMap.get(logSlotNum);
        if (e != null && e.status() != PaxosLogSlotStatus.CLEARED) return e.request().command();
        return null;
    }

    /**
     * Return the index of the first non-cleared slot in the server's local
     * log.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int firstNonCleared() {
        // Your code here...

        int idx = 1;
        int nCount = replicatedLogMap.size();
        int curCount = 0;
        while (curCount < nCount) {
            PaxosLogEntry e = replicatedLogMap.get(idx);
            if (e== null || e.status() != PaxosLogSlotStatus.CLEARED) {
                return idx;
            }
            if (e.status() == PaxosLogSlotStatus.CLEARED) {
                curCount++;
            }
            idx++;
        }
        return idx;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log. If
     * there are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int lastNonEmpty() {
        // Your code here...
        int nonEmpty = 0;

        for (Map.Entry<Integer, PaxosLogEntry> entry: replicatedLogMap.entrySet()) {
            PaxosLogEntry e = entry.getValue();
            if (e.status() != PaxosLogSlotStatus.EMPTY && entry.getKey() > nonEmpty) {
                nonEmpty = entry.getKey();
            }
        }

        return nonEmpty;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosRequest(PaxosRequest m, Address sender) {
        // TODO: handle paxos request ...
        /*int lastSeqNumber = app.getLastSequenceNumber(sender);
        if (lastSeqNumber != -1 && lastSeqNumber == m.sequenceNum()) {
            executeState(m, true, true);
            return;
        }*/
        if (acceptorNode.ballot_num != null) {
            replicaNode.handlePaxosRequest(m, sender);
        }
    }

    private synchronized void handleDecisionMsg(DecisionMsg m, Address sender) {

        replicaNode.handleDecisionMsg(m, sender);
    }

    private synchronized void handleBatchDecisionMsg(BatchDecisionMsg m, Address sender) {
        for(DecisionMsg d: m.missingDecisions()) {
            replicaNode.handleDecisionMsg(d, sender);
        }
    }
    private void handleP1aMessage(P1aMessage m, Address sender) {
        //////printLogInfo(address().toString() + " P1a message from scout:" + m.senderId().toString() +  " from " + sender.toString());
        acceptorNode.handleP1aMessage(m, sender);
    }

    private void handleP1bMessage(P1bMessage m, Address sender) {
        leaderNode.handleP1bMessage(m, sender);
    }

    private void handleP2aMessage(P2aMessage m, Address sender) {
        acceptorNode.handleP2aMessage(m, sender);
    }

    private void handleBatchP2aMessage(BatchP2aMessage m, Address sender) {
        acceptorNode.handleBatchP2aMessage(m, sender);
    }

    private void handleP2bMessage(P2bMessage m, Address sender) {
        leaderNode.handleP2bMessage(m, sender);
    }
    private void handleBatchP2bMessage(BatchP2bMessage m, Address sender) {
        leaderNode.handleBatchP2bMessage(m, sender);
    }

    private void handleProposalMsg(ProposalMsg m, Address sender) {
        leaderNode.handleProposalMsg(m, sender);
    }
    // TODO: your message handlers ...

    private void handleHeartbeatMsg(HeartbeatMsg m , Address sender) {
        serverHBMap.put(sender, hbCheckTimerTick);
        if (!leaderNode.isActive) {
            if (m.isLeaderActive() /*&& m.ballot_num().compareTo(acceptorNode.ballot_num) >= 0*/) {
                send(new LogStateMsg(replicaNode.slot_out, lastClearedSlot), sender);
            }
        }
        if (m.minSlotOut() > 1 && (m.minSlotOut() - lastClearedSlot) > 10) {
            //printLogInfoGC(address().toString() + " start clearing " + (m.minSlotOut() - lastClearedSlot));
            removeOldRequests(m.minSlotOut());
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    // TODO: your time handlers ...
    private void onHeartbeatTimer(HeartbeatTimer t) {
        if (leaderNode.isActive) {
            for (int i = 0; i < servers.length; i++) {
                if (!address().equals(servers[i])) {
                    send(new HeartbeatMsg(address(), minSlotOut,
                            leaderNode.ballot_number, leaderNode.isActive), servers[i]);
                }
            }
            set(t, HeartbeatTimer.HEARTBEAT_RETRY_MILLIS);
        }
    }



    private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
        hbCheckTimerTick++;
        if (acceptorNode.ballot_num != null && startHBCheck && !address().equals(acceptorNode.ballot_num.address())) {
            Integer val = serverHBMap.get(acceptorNode.ballot_num.address());
            if (val != null && val.intValue() < (hbCheckTimerTick - 2)) {
                ////printLogInfo(address().toString() + " did not get heartbeat from leader " + curLeaderId);
                startHBCheck = false;
                leaderNode.processPreemption(acceptorNode.ballot_num);
                return;
            }
        }
        set(t, HeartbeatCheckTimer.HEARTBEAT_CHECK_RETRY_MILLIS);
    }

    private void onP1aTimer(P1aTimer t) {
        if (!leaderNode.hasP1aResponded(t.msg())) {
            HashSet<Address> s = leaderNode.getScoutWaitset(t.msg());
            for(Address a : s) {
                send(t.msg(), a);
            }
            set(t, P1aTimer.P1A_RETRY_MILLIS);
        }
    }

    private void onP2aTimer(P2aTimer t) {
        if (!leaderNode.hasP2aResponded(t.msg())) {
            HashSet<Address> s = leaderNode.getCmdrWaitset(t.msg().senderId());
            if (leaderNode.ballot_number.compareTo(acceptorNode.ballot_num) >= 0) {
                for (Address a : s) {
                    send(t.msg(), a);
                }
                set(t, P2aTimer.P2A_RETRY_MILLIS);
            } else {
                leaderNode.exitCommander(t.msg().senderId());
            }
        }
    }

    private void onBatchP2aTimer(BatchP2aTimer t) {
        if (!leaderNode.hasBatchP2aResponded(t.msg())) {
            HashSet<Address> s = leaderNode.getCmdrWaitset(t.msg().senderId());
            if (leaderNode.ballot_number.compareTo(acceptorNode.ballot_num) >= 0) {
                for (Address a : s) {
                    send(t.msg(), a);
                }
                set(t, BatchP2aTimer.P2A_RETRY_MILLIS);
            }
        }
    }

    private void onProposeTimer(ProposeTimer t) {
        if (decisions.containsKey(t.msg().slot_number()) ) {
            return;
        }


        Address prevReceiver = t.to();
        Address curReceiver = acceptorNode.ballot_num.address();
        if (!curReceiver.equals(prevReceiver)) {
            t.to(curReceiver);
            //printLogInfo(address().toString() + " propose timer fired " + curReceiver);
        }
        if (address().equals(curReceiver)) {
            leaderNode.handleProposalMsg(t.msg(), null);
        } else {
            send(t.msg(), t.to());
            set(t, ProposeTimer.PROPOSE_RETRY_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    // TODO: add utils here ...
    public void sendMessageServer(MsgType type, Message m, Address to) {
        switch (type) {
            case P1B: {
                P1bMessage curMsg = (P1bMessage) m;
                if (to.equals(address())) {
                    leaderNode.handleP1bMessage(curMsg, to);
                } else {
                    send(curMsg, to);
                }
                break;
            }
            case P2B: {
                P2bMessage curMsg = (P2bMessage) m;
                if (to.equals(address())) {
                    leaderNode.handleP2bMessage(curMsg, to);
                } else {
                    send(curMsg, to);
                }
                break;
            }
            case BATCH_P2B: {
                BatchP2bMessage curMsg = (BatchP2bMessage) m;
                if (to.equals(address())) {
                    leaderNode.handleBatchP2bMessage(curMsg, to);
                } else {
                    send(curMsg, to);
                }
                break;
            }
            case PROPOSAL: {
                ProposalMsg curMsg = (ProposalMsg) m;
                send(curMsg, acceptorNode.ballot_num.address());
                set(new ProposeTimer(curMsg, acceptorNode.ballot_num.address()), ProposeTimer.PROPOSE_RETRY_MILLIS);
                break;
            }
            case P1A:{
                P1aMessage curMsg = (P1aMessage) m;
                for(int i = 0; i < servers.length; i++) {
                    if (address().equals(servers[i])) {
                        acceptorNode.handleP1aMessage(curMsg, address());
                    } else {
                        send(curMsg, servers[i]);
                    }
                }
                set(new P1aTimer(curMsg), P1aTimer.P1A_RETRY_MILLIS );
                break;
            }
            case P2A: {
                P2aMessage curMsg = (P2aMessage) m;
                for(int i = 0; i < servers.length; i++) {
                    if (address().equals(servers[i])) {
                        acceptorNode.handleP2aMessage(curMsg, address());
                    } else {
                        send(curMsg, servers[i]);
                    }
                }
                set(new P2aTimer(curMsg), P2aTimer.P2A_RETRY_MILLIS);
                break;
            }
            case BATCH_P2A: {
                BatchP2aMessage curMsg = (BatchP2aMessage) m;
                for(int i = 0; i < servers.length; i++) {
                    if (address().equals(servers[i])) {
                        acceptorNode.handleBatchP2aMessage(curMsg, address());
                    } else {
                        send(curMsg, servers[i]);
                    }
                }
                set(new BatchP2aTimer(curMsg), P2aTimer.P2A_RETRY_MILLIS);
                break;
            }
            case DECISION:{
                DecisionMsg curMsg = (DecisionMsg) m;
                for(int i = 0; i < servers.length; i++) {
                    if (address().equals(servers[i])) {
                        replicaNode.handleDecisionMsg(curMsg, address());
                    } else {
                        send(curMsg, servers[i]);
                    }
                }
                break;
            }
            case HEART_BEAT: {
                if (leaderNode.isActive) {
                    HeartbeatMsg curMsg = new HeartbeatMsg(address(),
                            replicaNode.slot_out, leaderNode.ballot_number,true);
                    for(int i = 0; i < servers.length; i++) {
                        if (address().equals(servers[i])) {
                            send(curMsg, servers[i]);
                            set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_RETRY_MILLIS);
                        }
                    }
                }
            }
            default:
                break;
        }

    }

    public Address[] getActiveServers() { return servers;  }
    public PaxosLogEntry setLogEntry(int slotNumber, PaxosLogSlotStatus s, PaxosRequest r) {

        PaxosLogEntry e = replicatedLogMap.get(slotNumber);
        if (e == null) {
            e = new PaxosLogEntry(r, s, slotNumber);
        } else {
            e.status(s);
            e.request(r);
        }
        replicatedLogMap.put(slotNumber, e);
        return  e;
    }



    public synchronized AMOResult executeState(PaxosRequest r, boolean resend, boolean readOnly) {
        Address client = r.address();
        AMOCommand amoCommand = new AMOCommand(r.address(), r.sequenceNum(), r.command());
        if (app.alreadyExecuted(amoCommand)) {
            return null;
        }
        AMOResult res = null;
        if (readOnly) {
            res = app.executeReadOnly(amoCommand);
        }
        else {
            res = doRequest(amoCommand);
        }
        try {
            PaxosReply pr = null;
            pr = new PaxosReply(client.toString(), r.sequenceNum(), res.internalRes());
            send(pr, client);

            ////printLogInfo1(address().toString() + " executing " + r.toString() + " and slot out is " + replicaNode.slot_out);
            //printLogInfo(address().toString() + " sent response " + pr.toString()+ " for " + r.toString() + " to client" + client.toString());
            return res;
        } catch (Exception e) {
            //printLogInfo("exception thrown " + e.getMessage() + " req:" + r.toString() + " client:" + client);
        }
        return null;
    }

    private AMOResult doRequest (AMOCommand cmd) {
        try {
            AMOResult res = null;
            res = app.execute(cmd);

            return res;
        } catch (Exception e) {
            //////printLogInfo("exception thrown at doRequest for " + this.address().toString() + " " + e.getMessage());
        }
        return null;
    }

    @Data
    private class Acceptor implements Serializable {
        private String id_;
        private Ballot ballot_num;
        private HashMap<Integer, Pvalue> acceptedMap = new HashMap<>();
        protected Acceptor(@NonNull String id) {
            this.id_ = id;
        }

        protected void init() {
            ballot_num = null;
            acceptedMap.clear();
        }


        protected void handleP1aMessage(P1aMessage m, Address sender) {
            for(Map.Entry<Integer,PaxosRequest> e: m.decisions().entrySet()){
                if (decisions.containsKey(e.getKey())) continue;
                replicaNode.handleDecisionMsg(new DecisionMsg(sender.toString(),e.getKey(), e.getValue()), null);
            }
            if (ballot_num == null || m.ballot_number().compareTo(ballot_num) > 0) {
                ballot_num = m.ballot_number();
            }
            sendMessageServer(MsgType.P1B, new P1bMessage(id_, ballot_num, acceptedMap, decisions, m.senderId()), sender);
            if (leaderNode.isActive && m.ballot_number().compareTo(leaderNode.ballot_number) > 0 ) {
                leaderNode.switchToFollower(m.ballot_number());
            }
        }

        protected void handleP2aMessage(P2aMessage m, Address sender) {
            if (ballot_num == null || m.ballot_number().compareTo(ballot_num) >= 0) {
                ballot_num = m.ballot_number();
                if (/*!decisions.containsKey(m.slot_number()) &&*/ m.slot_number() > lastClearedSlot) {
                    if (acceptedMap.containsKey(m.slot_number())) {
                        Pvalue v = acceptedMap.get(m.slot_number());
                        if (m.ballot_number().compareTo(v.ballot()) > 0) {
                            v = null;
                            acceptedMap.put(m.slot_number(),new Pvalue(m.slot_number(), m.ballot_number(), m.request()));
                        }
                    } else {
                        acceptedMap.put(m.slot_number(),new Pvalue(m.slot_number(), m.ballot_number(), m.request()));
                    }
                }
                setLogEntry(m.slot_number(), PaxosLogSlotStatus.ACCEPTED, m.request());
            }

            sendMessageServer(MsgType.P2B, new P2bMessage(id_, ballot_num, m.slot_number(), m.senderId()), sender);
            if (leaderNode.isActive && m.ballot_number().compareTo(leaderNode.ballot_number) > 0 ) {
                leaderNode.switchToFollower(m.ballot_number());
            }
        }


        protected void handleBatchP2aMessage(BatchP2aMessage p, Address sender) {
            if (ballot_num == null || p.ballot_number().compareTo(ballot_num) >= 0) {
                ballot_num = p.ballot_number();
                for(P2aMessage m: p.p2aMessages()) {
                    if (!decisions.containsKey(m.slot_number()) && m.slot_number() > lastClearedSlot) {
                        if (acceptedMap.containsKey(m.slot_number())) {
                            Pvalue v = acceptedMap.get(m.slot_number());
                            if (m.ballot_number().compareTo(v.ballot()) > 0) {
                                v = null;
                                acceptedMap.put(m.slot_number(),
                                        new Pvalue(m.slot_number(), m.ballot_number(),
                                                m.request()));
                            }
                        } else {
                            acceptedMap.put(m.slot_number(),
                                    new Pvalue(m.slot_number(), m.ballot_number(), m.request()));
                        }
                    }
                    setLogEntry(m.slot_number(), PaxosLogSlotStatus.ACCEPTED, m.request());
                }
            }

            sendMessageServer(MsgType.BATCH_P2B, new BatchP2bMessage(id_, ballot_num,p.p2aMessages(), p.senderId()), sender);
        }
    }
    @Data
    private class Leader implements Serializable{
        private String id_;
        private Ballot ballot_number;
        private boolean isActive;
        private Integer maxSlotNumber;
        private HashMap<String, Commander> commanderMap = new HashMap();
        private Scout scNode = null;
        private Integer batchid;
        protected Leader(@NonNull String id) {
            this.id_ = id;
        }

        protected void init() {
            isActive = false;
            batchid = 1;
            maxSlotNumber = 1;
            commanderMap.clear();
            ballot_number = new Ballot(0, address());
            proposalSet.clear();

            //create scout
            String scoutId= id_ + "-sc:" + ballot_number;
            scNode = new Scout(scoutId, ballot_number);
            ////printLogInfo("scout created:" + scoutId + " at line 602");
            scNode.init();

        }

        protected void handleProposalMsg(ProposalMsg m, Address sender) {
            if (sender != null) {
                //printLogInfo(address().toString() + " received proposal " + m.toString() + " from " + sender.toString());
            }
            if (m.slot_number() <= lastClearedSlot) return;
            if (isActive && decisions.containsKey(m.slot_number())) {
                if (sender != null) {
                    //printLogInfo("replay the decision for slot " + m.toString() + " on " + sender.toString());
                    send(new DecisionMsg(address().toString(), m.slot_number(),decisions.get(m.slot_number()) ), sender);
                    return;
                }
            }

            if (!proposalSet.containsKey(m.slot_number())) {
                //printLogInfo(address().toString() + " added proposal " + m.toString());
                proposalSet.put(m.slot_number(), m.request());
                if (isActive) {
                    //create commander
                    String  commanderId = id_ +"-cmdr:" + ballot_number + ":" + m.slot_number() ;
                    Commander cmdrNode = new Commander(commanderId,  ballot_number, m.slot_number(), m.request());
                    commanderMap.put(commanderId, cmdrNode);
                    //////printLogInfo("commander created:" + commanderId + " at line 615");
                    cmdrNode.init();
                    return;
                } else {
                    if (acceptorNode.ballot_num != null && !acceptorNode.ballot_num.address().equals(address()))
                    {
                        //printLogInfo(address().toString() + " sending proposal to " + acceptorNode.ballot_num.address().toString());
                        sendMessageServer(MsgType.PROPOSAL, m, acceptorNode.ballot_num.address());
                    }
                    return;
                }
            }
        }

        protected void handleAdoptedMsg(AdoptedMsg m) {
            if (ballot_number.compareTo(m.ballot_number()) == 0) {
                //printLogInfo(m.senderId() + " adopted");
                for (Map.Entry<Integer,Pvalue> e: m.acceptedMap().entrySet()){
                    if (e.getKey() < replicaNode.slot_out) continue;
                    if (decisions.containsKey(e.getKey())) continue;
                    proposalSet.put(e.getKey(), e.getValue().paxosRequest());
                }
                //HashSet<P2aMessage> p2aSet = new HashSet<>();
                for (int s : proposalSet.keySet()) {

                    if (!decisions.containsKey(s)) {
                        /*String commanderId = id_ + "-cmdr:" + ballot_number + ":b" + batchid;
                        P2aMessage p = new P2aMessage(commanderId, ballot_number,s, proposalSet.get(s));
                        p2aSet.add(p);*/
                        String commanderId = id_ + "-cmdr:" + ballot_number + ":" + s;
                        Commander cmdrNode = new Commander(commanderId, ballot_number, s,proposalSet.get(s));
                        commanderMap.put(commanderId, cmdrNode);
                        //printLogInfo("commander created:" + commanderId + " for " + cmdrNode.request.toString());
                        cmdrNode.init();
                    }
                }
                /*if (p2aSet.size() > 0 ){
                    String commanderId = id_ + "-cmdr:" + ballot_number + ":b" + batchid;
                    Commander cmdrNode = new Commander(commanderId, ballot_number,p2aSet);
                    commanderMap.put(commanderId, cmdrNode);
                    batchid++;
                    ////printLogInfo("commander created:" + commanderId + " for " + cmdrNode.request.toString());
                    cmdrNode.batchinit();
                }*/
                isActive = true;
                sendMessageServer(MsgType.HEART_BEAT, null, null);
                exitScout(m.senderId());
            }
        }

        protected void switchToFollower(Ballot b) {
            if (leaderNode.isActive && leaderNode.ballot_number.compareTo(b) < 0) {
                startHBCheck = true;
                isActive = false;
                //printLogInfo(address().toString() + " switched to follower for " + b.address().toString());
            }
        }
        protected void handlePreemptedMsg(PreemptedMsg m) {
            isActive = false;
            if (m.ballot_number().compareTo(ballot_number) > 0 ) {
                startHBCheck = true;
                //printLogInfo(m.senderId() + " preempted for " + m.ballot_number());
                exitScout(m.senderId());
            }
            exitCommander(m.senderId());

        }

        protected void processPreemption(Ballot b) {
            ballot_number = new Ballot(ballot_number.sequenceNum() + 1, address());
            //create scout
            String  scoutId = id_ + "-sc:" + ballot_number;
            scNode = new Scout(scoutId, ballot_number);
            //printLogInfo("scout created:" +scoutId);
            scNode.init();
        }
        protected void handleP1bMessage(P1bMessage m, Address sender) {
            if (scNode != null && scNode.id_.equals(m.requestorId())) {
                scNode.handleP1bMessage(m, sender);
            } else {
                //printLogInfo(m.requestorId() + " was deleted");
            }
        }

        protected void handleP2bMessage(P2bMessage m, Address sender) {
            Commander cmdr = commanderMap.get(m.requestorId());
            if (cmdr != null) {
                cmdr.handleP2bMessage(m, sender);
            }
        }

        protected void handleBatchP2bMessage(BatchP2bMessage m, Address sender) {
            Commander cmdr = commanderMap.get(m.requestorId());
            if (cmdr != null) {
                cmdr.handleBatchP2bMessage(m, sender);
            }
        }

        protected boolean hasP1aResponded(P1aMessage m) {
            if (scNode != null && scNode.id_.equalsIgnoreCase(m.senderId())) {
                if ((2 * scNode.waitAddress.size()) < getActiveServers().length) {
                    return true;
                } else {
                    return false;
                }
            }
            return true;
        }
        protected HashSet<Address> getScoutWaitset(P1aMessage m) {
            if (scNode != null && scNode.id_.equalsIgnoreCase(m.senderId())) {
                return scNode.waitAddress;
            }
            return null;

        }
        protected HashSet<Address> getCmdrWaitset(String id) {
            Commander c = commanderMap.get(id);
            if (c != null) { return c.waitAddress;}
            return null;
        }

        protected boolean hasP2aResponded(P2aMessage m) {
            Commander c = commanderMap.get(m.senderId());
            if (c != null) {
                if ((2 *c.waitAddress.size()) < getActiveServers().length ) {
                    return true;
                }
                return false;
            }
            return true;
        }

        protected boolean hasBatchP2aResponded(BatchP2aMessage m) {
            Commander c = commanderMap.get(m.senderId());
            if (c != null) {
                if ((2 *c.waitAddress.size()) < getActiveServers().length ) {
                    return true;
                }
                return false;
            }
            return true;
        }

        protected void handleDecisionMsg(DecisionMsg m) {
            //printLogInfo(address().toString() + " sending decision for slot " + m.toString());
            if (m.slot_number() >= maxSlotNumber) maxSlotNumber = m.slot_number();
            sendMessageServer(MsgType.DECISION, m, null);
            if (isActive) {
                exitCommander(m.senderId());
                //printLogInfo(m.senderId() + " exited for " + m.request().toString());
            }
        }

        protected void handleBatchDecisionForP2bMsg(BatchDecisionMsg m) {
            sendMessageServer(MsgType.BATCH_DECISION, m, null);
            if (isActive) {
                DecisionMsg d = m.missingDecisions().iterator().next();
                for(Address a: servers) {
                    if (address().equals(a)) {
                        handleBatchDecisionMsg(m, null);
                    } else {
                        send(m, a);
                    }
                }
                exitCommander(d.senderId());
                ////printLogInfo(m.senderId() + " exited for " + m.request().toString());
            }
        }

        protected void exitCommander(String id) {
            Commander cmdr = commanderMap.remove(id);
            if (cmdr != null) {
                cmdr.clear();
                cmdr = null;
            }
        }

        protected void exitScout(String id ) {
            if (scNode != null) {
                //printLogInfo( scNode.id_ + " scout exited");
                scNode.clear();
                scNode = null;
            }
        }
    }

    @Data
    private class Scout implements Serializable {
        private String id_;
        private Ballot ballot_number;
        private HashMap<Integer, Pvalue> pvalueMap = new HashMap<>();
        private HashSet<Address> waitAddress = new HashSet<>();
        protected Scout(@NonNull String id, Ballot b) {
            id_ = id;
            ballot_number = b;
            waitAddress.clear();
        }

        protected void init() {
            pvalueMap.clear();
            for (Address a : servers) {
                waitAddress.add(a);
            }
            sendMessageServer(MsgType.P1A,new P1aMessage(id_, ballot_number, decisions), null);
        }

        protected void handleP1bMessage(P1bMessage m, Address sender) {
            if (((2 *waitAddress.size()) < getActiveServers().length)) {
                return;
            }
            for (Map.Entry<Integer, PaxosRequest> x: m.decisionMap().entrySet()) {
                if (x.getKey() < lastClearedSlot) continue;
                if (decisions.containsKey(x.getKey())) continue;
                replicaNode.handleDecisionMsg(new DecisionMsg(id_, x.getKey(),x.getValue()), null);
            }
            if (waitAddress.contains(sender)) {
                waitAddress.remove(sender);
                if (ballot_number.compareTo(m.ballot_number()) == 0 ) {

                    for(Map.Entry<Integer, Pvalue> e: m.acceptedMap().entrySet()) {
                        if (e.getKey() < replicaNode.slot_out) continue;
                        Pvalue v = pvalueMap.get(e.getKey());
                        if (v != null) {
                            if(v.ballot().compareTo(e.getValue().ballot()) < 0) {
                                pvalueMap.put(e.getKey(), e.getValue());
                            }
                        } else {
                            pvalueMap.put(e.getKey(), e.getValue());
                        }
                    }
                    if ((2 * waitAddress.size()) < getActiveServers().length) {
                        leaderNode.handleAdoptedMsg(
                                new AdoptedMsg(id_, ballot_number, pvalueMap));
                    }
                } else {
                    leaderNode.handlePreemptedMsg(new PreemptedMsg(id_, m.ballot_number()));
                }
            }

        }
        public void clear() {
            ballot_number = null;
            waitAddress.clear();
            pvalueMap.clear();
        }

    }
    @Data
    private class Commander implements Serializable{
        private String id_;
        private Address[] acceptors;
        private Ballot ballot_number;
        private Integer slot_number;
        private PaxosRequest request;
        private HashSet<Address> waitAddress = new HashSet<>();
        private HashSet<P2aMessage> p2aSet = new HashSet<>();
        protected Commander(@NonNull String id, Ballot b, Integer s, PaxosRequest r) {
            id_ = id;
            request = r;
            ballot_number = b;
            slot_number = s;
        }

        protected Commander(@NonNull String id, Ballot b, HashSet<P2aMessage> pSet) {
            id_ = id;
            ballot_number = b;
            p2aSet = pSet;
        }

        protected void init() {
            for (Address a : getActiveServers()) {
                waitAddress.add(a);
            }
            sendMessageServer(MsgType.P2A, new P2aMessage(id_, ballot_number, slot_number, request), null);
        }

        protected void batchinit() {
            for (Address a : getActiveServers()) {
                waitAddress.add(a);
            }
            sendMessageServer(MsgType.BATCH_P2A, new BatchP2aMessage(id_, ballot_number, p2aSet), null);
        }
        protected void handleP2bMessage(P2bMessage m, Address sender) {
            if (((2 *waitAddress.size()) < getActiveServers().length) ) {
                return;
            }
            if (waitAddress.contains(sender)) {
                waitAddress.remove(sender);
                if (ballot_number.compareTo(m.ballot_number()) == 0 ) {
                    if (((2 * waitAddress.size()) < getActiveServers().length)) {
                        leaderNode.handleDecisionMsg(
                                new DecisionMsg(id_, slot_number, request));
                    }
                } else {
                    leaderNode.handlePreemptedMsg(new PreemptedMsg(id_, m.ballot_number()));
                }
            }
        }

        protected void handleBatchP2bMessage(BatchP2bMessage m, Address sender) {
            if (((2 *waitAddress.size()) < getActiveServers().length) ) {
                return;
            }
            if (waitAddress.contains(sender)) {
                waitAddress.remove(sender);
                if (ballot_number.compareTo(m.ballot_number()) == 0 ) {
                    if (((2 * waitAddress.size()) < getActiveServers().length)) {
                        HashSet<DecisionMsg>dSet = new HashSet<>();
                        for(P2aMessage p: m.p2aMessages()) {
                            DecisionMsg d = new DecisionMsg(id_, p.slot_number(), p.request());
                            dSet.add(d);
                        }
                       leaderNode.handleBatchDecisionForP2bMsg(new BatchDecisionMsg(dSet));

                    }
                } else {
                    leaderNode.handlePreemptedMsg(new PreemptedMsg(id_, m.ballot_number()));
                }
            }
        }
        protected void clear() {
            request = null;
            ballot_number = null;
            acceptors = null;
        }
    }

    @Data
    private class Replica implements Serializable{
        private String id_;
        private int slot_out;
        private int slot_in;



        protected Replica(@NonNull String id) {
            id_ = id;
        }

        protected void init() {
            //printLogInfo(address().toString() + " init called ");
            slot_out = slot_in = 1;
            decisions.clear();
            requests.clear();
            reProposedSet.clear();
            requestProcessed.clear();
        }

        private boolean isDuplicateRequest(PaxosRequest r) {
            Address client = r.address();
            Integer incomingSeqNum = r.sequenceNum();
            Integer existingSeqNum = requestProcessed.get(client);
            if (existingSeqNum != null && incomingSeqNum < existingSeqNum) {
                return true;
            }
            return false;
        }



        protected void handlePaxosRequest(PaxosRequest m, Address sender) {

            /*int lastSeqNumber = app.getLastSequenceNumber(sender);
            if (lastSeqNumber != -1 && lastSeqNumber == m.sequenceNum()) {
                executeState(m, true, true);
                return;
            }*/
            if (!isDuplicateRequest(m) && !proposalSet.containsValue(m)) {
                requests.add(m);
            }
            while (requests.size() > 0) {
                PaxosRequest r = requests.poll();
                requestProcessed.put(sender, m.sequenceNum());
                //printLogInfo(address().toString() + " got request " + r.toString());
                propose(r);
            }
        }

        protected void handleDecisionMsg(DecisionMsg m, Address sender) {
            if (m.slot_number() < slot_out) return;
            decisions.put(m.slot_number(), m.request());
            //printLogInfo(address().toString() + " received decision for " + m.slot_number() + " when slot_out is " + slot_out + " size is:" + decisions.size());
            PaxosRequest decisionReq = null;
            while ((decisionReq = decisions.get(slot_out)) != null) {
                PaxosRequest proposalReq = proposalSet.get(slot_out);

                if (proposalReq != null && !proposalReq.equals(decisionReq)) {
                    //printLogInfo(address().toString() + " reproposing for slot" + slot_out + " "+ proposalReq.toString());
                    reProposedSet.add(proposalReq);
                    propose(proposalReq);
                }
                if (reProposedSet.contains(decisionReq)) {
                    reProposedSet.remove(decisionReq);
                }
                proposalSet.remove(slot_out);
                perform(decisionReq);
            }
            //printLogInfo(address().toString() + " exiting decision loop when slot_out is " + slot_out + " d size:" + decisions.size());
        }

        private void perform(PaxosRequest r) { //r1 --> slot 3 and slot 5 slot_out 6
            boolean bExecute = true;
            for (int i = lastClearedSlot + 1; i < slot_out; i++) {
                if (r.equals(decisions.get(i))) {
                    bExecute = false;
                    break;
                }
            }

            setLogEntry(slot_out, PaxosLogSlotStatus.CHOSEN, r);
            if (bExecute) executeState(r, false, false);
            slot_out += 1;
        }

        private void propose(PaxosRequest r) {
            //printLogInfo(address().toString() + " propose call for " + r.toString() + " decisionmap size:" + decisions.size() + " proposalset size:" + proposalSet.size());
            if (!decisions.containsValue(r)) {
                if (proposalSet.containsValue(r) && !reProposedSet.contains(r)) {
                    return;
                }

                while (true) {
                    if (!decisions.containsKey(slot_in) && !proposalSet.containsKey(slot_in)) {
                        leaderNode.handleProposalMsg(new ProposalMsg(address(), slot_in, r),null);
                        slot_in += 1;
                        break;
                    }
                    slot_in += 1;
                }
            } else {
                //printLogInfo(r.toString() + " already processed");
                int slot = getDecidedSlot(r);
                if (slot != -1 && slot < slot_out)executeState(r, true, false);

                //printLogInfo(r.toString() + " send response again");
            }

        }
        private int getDecidedSlot(PaxosRequest r) {
            int retval = -1;
            for(Map.Entry<Integer, PaxosRequest> e: decisions.entrySet()) {
                if (r.equals(e.getValue())) {
                    retval = e.getKey();
                    break;
                }
            }
            return retval;
        }

    }
    // Garbage Collection
    private void handleLogStateMsg(LogStateMsg m, Address sender) {
        if (leaderNode.isActive) {
            if (m.lastCleared() > lastClearedSlot) {
                //printLogInfoGC(sender.toString() + " seems to clear till " + m.lastCleared());
                removeOldRequests(minSlotOut);
                minSlotOut = -1;
            }
            followerSlotMap.put(address(), replicaNode.slot_out);

            int diff = replicaNode.slot_out - m.slotOut();
            if (diff > 5) {
                //printLogInfoGC(address().toString() + " slot out:" + replicaNode.slot_out + " "+ sender.toString() + " slot out:" + m.slotOut());
                int curSlot = m.slotOut();
                HashSet<DecisionMsg> missingDecisions = new HashSet<>();
                while (curSlot < replicaNode.slot_out) {
                    DecisionMsg d = new DecisionMsg(address().toString(), curSlot, decisions.get(curSlot));
                    missingDecisions.add(d);
                    curSlot++;
                }
                //printLogInfoGC(address().toString() + " sending batch size " + missingDecisions.size() + " to " + sender.toString());
                send(new BatchDecisionMsg(missingDecisions), sender);
            } else {
                followerSlotMap.put(sender, m.slotOut());
            }
            if (followerSlotMap.size() == servers.length) {
                //printLogInfoGC( address().toString() + " got all slot outs");
                int minVal = replicaNode.slot_out;
                for(Entry<Address, Integer> e: followerSlotMap.entrySet()) {
                    if (e.getValue() < minVal) {
                        minVal = e.getValue();
                    }
                }

                if (minVal > 1) {
                    minSlotOut = minVal;
                }
                //printLogInfoGC(address().toString() + " decided min slot for " + minSlotOut);
                followerSlotMap.clear();
            }
        }

    }
    protected void removeOldRequests(int minSlot) {
        //printLogInfoGC(address().toString() + " clearing till " + minSlot);
        for (Iterator<Entry<Integer, Pvalue>> entryItr = acceptorNode.acceptedMap.entrySet().iterator();entryItr.hasNext() ;) {
            Entry<Integer, Pvalue> e = entryItr.next();
            if (e.getKey()< minSlot) {
                e.setValue(null);
                entryItr.remove();
            }
        }


        for (Iterator<Entry<Integer, PaxosLogEntry>> entryItr = replicatedLogMap.entrySet().iterator(); entryItr.hasNext();) {
            Entry<Integer, PaxosLogEntry> e = entryItr.next();
            if (e.getKey() < minSlot) {
                PaxosLogEntry logEntry = e.getValue();
                logEntry.status(PaxosLogSlotStatus.CLEARED);
                logEntry.request(null);
            }
        }
        for (Iterator<Entry<Integer, PaxosRequest>> entryItr = decisions.entrySet().iterator(); entryItr.hasNext();) {
            Entry<Integer, PaxosRequest> e = entryItr.next();
            if (e.getKey() < minSlot) {
                PaxosRequest r = e.getValue();
                e.setValue(null);
                entryItr.remove();
            }
        }
        lastClearedSlot = minSlot;
    }
    private  void printLogInfo(String s) {
        //LOG.info(s);
    }
    private  void printLogInfo1(String s) {
        //LOG.info(s);
    }
    private  void printLogInfoGC(String s) {
        //LOG.info(s);
    }

    public void print_decision() {
        //printLogInfo("printing decision for " + address().toString() + " decision size: " + decisions.size() + " log size:" + replicatedLogMap.size());
        for(Map.Entry<Integer,PaxosRequest> e: decisions.entrySet()) {
            //printLogInfo1(address().toString() + " decisionmap " +  e.getKey() + ":" + e.getValue());
        }
    }

    public void print_log() {
        //printLogInfo("printing log for " + address().toString() + " log size:" + replicatedLogMap.size());
        for(Map.Entry<Integer,PaxosLogEntry> e: replicatedLogMap.entrySet()) {
            PaxosLogEntry entry = e.getValue();
            if (entry != null)
            printLogInfo1(address().toString() + " log slot " +  e.getKey() + ":" + " status:" + entry.status() + (entry.request() != null ? entry.request().toString() : "null"));
            else {
                printLogInfo1(address().toString()  + " log slot " +  e.getKey() + ":hole");
            }
        }
    }
}
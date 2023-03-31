package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    //TODO: declare fields for your implementation ...
    private PaxosRequest curRequest;
    private Result curResult;
    private int nextSequenceNum;
    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
        
        // TODO: initialize fields ...
        curRequest = null;
        curResult = null;
        nextSequenceNum = 1;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // TODO: send command ...
        curRequest = new PaxosRequest(this.address(), nextSequenceNum, operation);
        broadcast(curRequest, servers);
        //broadcastRequest(curRequest);
        set(new ClientTimer(curRequest), CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return curRequest.sequenceNum() != nextSequenceNum;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        while (curRequest.sequenceNum() == nextSequenceNum) {
            wait();
        }
        return curResult;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // TODO: handle paxos server reply ...
        if (curRequest.sequenceNum() == nextSequenceNum &&
                m.sequenceNum() == nextSequenceNum) {
            //LOG.info(address().toString() + " received response " + m.toString());
            curResult = m.result();
            nextSequenceNum++;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
        PaxosRequest r = t.request();
        if (t.request().sequenceNum() == nextSequenceNum ) {
            broadcast(t.request(), servers);
            //broadcastRequest(t.request());
            set(t, CLIENT_RETRY_MILLIS);
        }
    }

    private synchronized void broadcastRequest(PaxosRequest r) {
        int len = servers.length;
        int i = servers.length;
        while (2*i > len ) {
            send(r, servers[i-1]);
            i--;
        }

    }

}

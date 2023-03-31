package dslabs.atmostonce;


import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.AppendResult;
import dslabs.kvstore.KVStore.GetResult;

import dslabs.kvstore.KVStore.KeyNotFound;
import dslabs.kvstore.KVStore.PutOk;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.java.Log;


@Log
@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    //TODO: declare fields for your implementation
    private HashMap<Address, AMOResult> requestMap = new HashMap<>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;
        //Hints: remember to check whether the command is executed before and update records
        /*if (alreadyExecuted(amoCommand)) {
            return null;
        }*/

        if (requestMap.containsKey(amoCommand.clientId())) {
            AMOResult amoResult = requestMap.get(amoCommand.clientId());
            if (amoResult.seqNumber() == amoCommand.seqNumber()) {
                return amoResult;
            }
        }

        Result res = application.execute(amoCommand.internalCmd());
        AMOResult amoResult = new AMOResult(amoCommand.seqNumber(), res);
        requestMap.put(amoCommand.clientId(), amoResult);
        return amoResult;
    }

    // copy constructor

    public AMOApplication(AMOApplication<Application> amoApplication)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Application application = amoApplication.application();
        this.application = (T) application.getClass().getConstructor(application.getClass()).newInstance(application);
        //Hints: remember to deepcopy all fields, especially the mutable ones
        this.requestMap.clear();

        //this.requestMap = (HashMap<Address, AMOResult>) amoApplication.requestMap.clone();
        for (Map.Entry<Address, AMOResult> entry : amoApplication.requestMap.entrySet()) {
            AMOResult res = entry.getValue();
            if (res.internalRes() instanceof AppendResult) {
                String val = ((AppendResult)res.internalRes()).value();
                requestMap.put(entry.getKey(), new AMOResult(res.seqNumber(), new AppendResult(val)));
            } else if (res.internalRes() instanceof GetResult) {
                String val = ((GetResult)res.internalRes()).value();
                requestMap.put(entry.getKey(), new AMOResult(res.seqNumber(), new GetResult(val)));
            } else if (res.internalRes() instanceof PutOk) {
                requestMap.put(entry.getKey(), new AMOResult(res.seqNumber(), new PutOk()));
            } else if (res.internalRes() instanceof KeyNotFound) {
                requestMap.put(entry.getKey(), new AMOResult(res.seqNumber(), new KeyNotFound()));
            } else {
                continue;
            }
        }
    }

    public Integer getRequestMapSize () {
        return this.requestMap.size();
    }
    public AMOResult executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }
        AMOCommand amoCommand = (AMOCommand) command;

        if (requestMap.containsKey(amoCommand.clientId())) {
            AMOResult amoResult = requestMap.get(amoCommand.clientId());
            if (amoResult.seqNumber() == amoCommand.seqNumber()) {
                return amoResult;
            }
        }
        return null;
    }

    public boolean alreadyExecuted(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        if (amoCommand != null && requestMap.containsKey(amoCommand.clientId())) {
            AMOResult amoResult = requestMap.get(amoCommand.clientId());
            return amoCommand.seqNumber() < amoResult.seqNumber();
        }
        return false;
    }
}

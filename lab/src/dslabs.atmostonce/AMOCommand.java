package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
    //TODO: implement your wrapper for command
    //Hints: think carefully about what information is required for server to check duplication
    //private final String clientId;
    private final Address clientId;
    private final int seqNumber;
    private final Command internalCmd;
}

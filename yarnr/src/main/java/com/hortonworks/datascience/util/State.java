package com.hortonworks.datascience.util;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

/**
 * Created by cstella on 9/17/14.
 */
public enum State {
    INITIALIZE
   ,START_CLIENT
   ,SHUTDOWN
    ;
    public byte[] toPayload()
    {
        return Ints.toByteArray(ordinal());
    }

    public static State fromPayload(byte[] payload)
    {
        return State.values()[Ints.fromByteArray(payload)];
    }
}

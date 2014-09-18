package com.hortonworks.datascience.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

/**
* Created by cstella on 9/17/14.
*/
public class ConnectionString
{
    private Iterable<String> containerHosts;
    private int tunnelPort;
    public ConnectionString(Iterable<String> containerHosts, int tunnelPort)
    {
        this.containerHosts = containerHosts;
        this.tunnelPort = tunnelPort;
    }
    public static ConnectionString from(String serialized)
    {
        Iterable<String> tokens = Splitter.on(':').split(serialized);
        int tunnelPort = Integer.parseInt(Iterables.getLast(tokens, ""));
        return new ConnectionString(Splitter.on(',').split(Iterables.getFirst(tokens, "")), tunnelPort);
    }

    public String getReachableHost()
    {
        return Iterables.getFirst(Iterables.filter(getContainerHosts(), Util.IS_REACHABLE), null);
    }
    public Iterable<String> getContainerHosts() {
        return containerHosts;
    }

    public void setContainerHosts(Iterable<String> containerHosts) {
        this.containerHosts = containerHosts;
    }

    public int getTunnelPort() {
        return tunnelPort;
    }

    public void setTunnelPort(int tunnelPort) {
        this.tunnelPort = tunnelPort;
    }

    public String serialize()
    {
        return Joiner.on(',').join(containerHosts) + ":" + tunnelPort;
    }

    public static Function<String, ConnectionString> TO_CONNECTION_STRING = new Function<String, ConnectionString>()
    {

        @Override
        public ConnectionString apply(@Nullable String input) {
            return from(input);
        }
    };

}

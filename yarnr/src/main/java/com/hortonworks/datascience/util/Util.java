package com.hortonworks.datascience.util;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

public enum Util
{
    INSTANCE;
    public static final String PREFIX="/parallelr";
    public static final int TUNNEL_PORT = 5555;
    public static final int R_PORT = 10175;
    public CuratorFramework connectToZookeeper(String connectString)
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
        client.start();
        return client;
    }
    public String getBasePath(String session)
    {
        return PREFIX + "/" + session;
    }
    public String getFullPath(String ... paths)
    {
        return Joiner.on('/').join(paths);
    }

    public static Predicate<String> IS_REACHABLE = new Predicate<String>()
    {

        @Override
        public boolean apply(@Nullable String input) {
            try {
                return InetAddress.getByName(input).isReachable(1000);
            } catch (IOException e) {
                return false;
            }
        }
    };

    public <U,T> Function<U,T>  noopCallback(Class<U> clazzu, Class<T> clazzt)
    {
        return new Function<U,T>()
        {
            @Override
            public T apply(@Nullable U input) {
                return null;
            }
        };
    }
}

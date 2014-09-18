package com.hortonworks.datascience;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.hortonworks.datascience.proxy.ReverseProxy;
import com.hortonworks.datascience.util.ConnectionString;
import com.hortonworks.datascience.util.State;
import com.hortonworks.datascience.util.Util;
import com.hortonworks.datascience.yarn.YarnClient;
import org.apache.commons.cli.ParseException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cstella on 9/17/14.
 */
public class YarnRClient
{
    private CuratorFramework zkClient = null;
    private YarnClient yarnClient = new YarnClient();
    private Iterable<ConnectionString> remoteMachines = null;
    private String zkQuorum;
    private String session;

    public YarnRClient(String session, String zkQuorum)
    {
        this.zkQuorum = zkQuorum;
        zkClient = Util.INSTANCE.connectToZookeeper(zkQuorum);
        this.session = session;
    }

    public void start(String dockerImage, int localPort, int numContainers) throws Exception {
        String basePath = Util.INSTANCE.getBasePath(session);
        if(zkClient.checkExists().forPath(basePath) != null)
        {
            System.out.println("Cleaning zookeeper @ " + basePath);
            zkClient.delete().deletingChildrenIfNeeded().forPath(basePath);
        }
        zkClient.create().creatingParentsIfNeeded().forPath(basePath);
        //run yarn job to start containers
        try {
            if(!runYarn(dockerImage, numContainers))
            {
                throw new RuntimeException("Unable to start yarn job to create containers, bailing prematurely.");
            }
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            throw new RuntimeException("Unable to start yarn job to create containers, bailing prematurely.", e);
        }
        //wait for all the containers to be allocated
        try {
            System.out.println("Waiting for the containers to be allocated and proxies to be setup...");
            remoteMachines = getHosts(session, numContainers);
        } catch (Exception e) {
            try {
                System.out.println("Cleaning up application...");
                e.printStackTrace();
                yarnClient.forceKillApplication();
            } catch (YarnException e1) {
                e1.printStackTrace(System.err);
            } catch (IOException e1) {
                e1.printStackTrace(System.err);
            }
            throw new RuntimeException("Unable to retrieve containers.", e);
        }

        //start the local proxy to set up the 2nd part of the reverse proxy
        try {
            startClient(remoteMachines, localPort);
        } catch (InterruptedException e) {
            try {
                System.out.println("Cleaning up application...");
                yarnClient.forceKillApplication();
            } catch (YarnException e1) {
                e1.printStackTrace(System.err);
            } catch (IOException e1) {
                e1.printStackTrace(System.err);
            }
            throw new RuntimeException("Unable to start proxy", e);
        }
        Thread t = new Thread(new Runnable()
        {

            @Override
            public void run() {

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
                //start the SNOW worker on each client
                for(ConnectionString cs : remoteMachines)
                {
                    String zkNode = Util.INSTANCE.getFullPath(Util.PREFIX
                                                             , session
                                                             , cs.serialize()
                                                             );
                    try {
                        zkClient.setData().forPath(zkNode, State.START_CLIENT.toPayload());
                    } catch (Exception e) {
                        try {
                            System.out.println("Cleaning up application...");
                            yarnClient.forceKillApplication();
                        } catch (YarnException e1) {
                            e1.printStackTrace(System.err);
                        } catch (IOException e1) {
                            e1.printStackTrace(System.err);
                        }
                        throw new RuntimeException("Unable to set zookeeper data to kill client.");
                    }
                }
            }
        });
        t.start();

    }

    public void kill() throws Exception {
        try {
            yarnClient.forceKillApplication();
        } catch (YarnException e1) {
            e1.printStackTrace(System.err);
        } catch (IOException e1) {
            e1.printStackTrace(System.err);
        }
        try
        {
            zkClient.delete().deletingChildrenIfNeeded().forPath(Util.INSTANCE.getBasePath(session));
        }
        catch(Exception e)
        {
            System.err.println("WARN: Unable to cleanup zookeeper.  " +
                    "Please make sure " + Util.INSTANCE.getBasePath(session) + " is cleaned up.");
        }
    }

    private void startClient(final Iterable<ConnectionString> remoteMachines
                             , final int localPort
                             ) throws InterruptedException {
        final StringBuffer config = new StringBuffer();
        for(ConnectionString remoteMachine : remoteMachines)
        {
            String remoteHostname = remoteMachine.getReachableHost();
            int tunnelPort = remoteMachine.getTunnelPort();
            //config.append("192.168.2.1 5555 127.0.0.1 8080 MUX=IN");
            config.append(Joiner.on(' ')
                                .join( "0.0.0.0"
                                     , localPort
                                     , remoteHostname
                                     , tunnelPort
                                     , "MUX=OUT"
                                     ) + "\n"

                         );
        }
        Thread reverseProxy = new Thread(new Runnable()
        {

            @Override
            public void run() {
                try {
                    new ReverseProxy().reload(new StringBufferInputStream(config.toString())
                                             , Util.INSTANCE.noopCallback(Integer.class, Void.class)
                                             , Util.INSTANCE.noopCallback(Integer.class, Void.class)
                                             );
                } catch (IOException e) {
                    throw new RuntimeException("Unable to load config");
                }
            }
        });
        reverseProxy.run();
        reverseProxy.join();
    }

    private Iterable<ConnectionString> getHosts(final String session, final int numContainers) throws Exception {
        final AtomicInteger cnt = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Watcher watcher = new Watcher()
        {

            @Override
            public void process(WatchedEvent watchedEvent) {
                int newCnt = cnt.addAndGet(1);
                System.out.println("Found host..." + watchedEvent);
                if(newCnt >= numContainers)
                {
                    countDownLatch.countDown();

                }
            }
        };
        zkClient.getChildren().usingWatcher(watcher).forPath(Util.INSTANCE.getBasePath(session));
        countDownLatch.await();
        return Iterables.transform( zkClient.getChildren()
                                            .forPath(Util.INSTANCE.getBasePath(session))
                                  , ConnectionString.TO_CONNECTION_STRING
                                  );
    }

    private boolean runYarn(  String dockerImage, int numContainers) throws IOException, YarnException, ParseException {
        System.out.println("Running yarn job...");
        yarnClient.init("default",50, numContainers, session, zkQuorum, dockerImage,false);
        return yarnClient.run();
    }
}

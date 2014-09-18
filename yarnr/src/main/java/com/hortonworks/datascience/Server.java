package com.hortonworks.datascience;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.hortonworks.datascience.proxy.ReverseProxy;
import com.hortonworks.datascience.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringBufferInputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cstella on 9/13/14.
 */
public class Server {
    private static enum Opts
    {
        HELP(OptionBuilder.withLongOpt("help")
                .withDescription("Print this message")
                .create("h")
                , "h"
        )

        ,QUORUM(OptionBuilder
            .withDescription("zookeeper quorum")
            .hasArg()
            .withArgName("CONNECT_STRING")
            .isRequired()
            .withLongOpt("zk_quorum")
            .create("q")
            , "q"
        )
        ,DOCKER_IMAGE(OptionBuilder
            .withDescription("Docker Image")
            .hasArg()
            .withArgName("image name")
            .withLongOpt("docker_image")
            .create("d")
            , "d"
        )
        ,SESSION(OptionBuilder
            .withDescription("Session Name")
            .hasArg()
            .withArgName("NAME")
            .isRequired()
            .withLongOpt("session")
            .create("e")
            , "e"
        )
        ;
        static Options options = new Options();
        static CommandLineParser parser = new PosixParser();
        static
        {
            for(Opts opt : values())
            {
                options.addOption(opt.option);
            }
        }
        String code;
        Option option;
        Opts(Option option, String code)
        {
            this.option = option;
            this.code = code;
        }
        public boolean has(CommandLine commandLine)
        {
            return commandLine.hasOption(code);
        }
        public String get(CommandLine commandLine)
        {
            return commandLine.getOptionValue(code);
        }
        public static void printHelp(PrintWriter pw)
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "Runner", options , true);

            formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH
                    , "Runner"
                    , null, options
                    , HelpFormatter.DEFAULT_LEFT_PAD
                    , HelpFormatter.DEFAULT_DESC_PAD
                    , null
                    , true
            );

        }
        public static CommandLine parse(String... argv)
        {
            try
            {
                return parser.parse( options, argv );
            }
            catch(ParseException ex)
            {
                ex.printStackTrace(System.err);
                printHelp(new PrintWriter(System.err));
                return null;
            }
        }
    }
    /*
     * TODO on server mode:
     * 1. set up reverse proxy
     * 2. set zookeeper node with host and port
     * 3. start client
     *
     * TODO on initialize:
     * 1. start slider
     * 2. read zookeeper node for hosts and ports
     * 3. setup client-side reverse proxy
     *
     *
     * TODO on command:
     * 1.
     */
    public static void main(String... argv) throws Exception {
        CommandLine cli = Opts.parse(argv);
        System.setProperty("java.net.preferIPv4Stack" , "true");
        String zkQuorum = Opts.QUORUM.get(cli);
        System.out.println("Using zk quorum of " + zkQuorum);
        CuratorFramework client = Util.INSTANCE.connectToZookeeper(zkQuorum);
        String sessionName = Opts.SESSION.get(cli);
        String dockerImage = null;
        if(Opts.DOCKER_IMAGE.has(cli))
        {
            dockerImage = Opts.DOCKER_IMAGE.get(cli);
            if(dockerImage.isEmpty() )
            {
                dockerImage = null;
            }
        }
        server(dockerImage, client, sessionName);

    }

    public static Iterable<String> getAddress() throws SocketException {
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        List<String> ret = new ArrayList<String>();
        while(e.hasMoreElements())
        {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements())
            {
                InetAddress i = (InetAddress) ee.nextElement();
                String s = i.getHostAddress();
                if(i.isLoopbackAddress() )
                {
                    continue;
                }
                else
                {
                    ret.add(s);
                }
            }
        }
        return ret;
    }


    public static void server( final String dockerImage
                             , final CuratorFramework zkClient
                             , final String sessionName
                             ) throws Exception
    {
        final AtomicInteger tunnelPort = new AtomicInteger();
        final AtomicInteger localPort = new AtomicInteger();
        final CountDownLatch tunnelPortLatch = new CountDownLatch(1);
        final CountDownLatch localPortLatch= new CountDownLatch(1);
        //String hadoopConfig.get("dfs.namenode.http-address");
        final StringBuffer config = new StringBuffer();
        config.append(Joiner.on(' ')
                .join( "0.0.0.0"
                        , Util.TUNNEL_PORT
                        , "0.0.0.0"
                        , Util.R_PORT
                        , "MUX=IN"
                ) + "\n"

        );
        final Function<Integer, Void> tunnelPortCallback = new Function<Integer, Void>()
        {

            @Override
            public Void apply(Integer input) {
                tunnelPort.set(input);
                System.out.println("Setting tunnel port to " + input);
                tunnelPortLatch.countDown();
                return null;
            }
        };
        final Function<Integer, Void> localPortCallback = new Function<Integer, Void>()
        {

            @Override
            public Void apply(Integer input) {
                localPort.set(input);
                System.out.println("Setting local port to " + input);
                localPortLatch.countDown();
                return null;
            }
        };
        final Thread reverseProxy = new Thread(new Runnable()
        {

            @Override
            public void run() {
                try {
                    new ReverseProxy().reload( new StringBufferInputStream(config.toString())
                            , tunnelPortCallback
                            , localPortCallback
                    );
                }
                catch (Exception e) {
                    throw new RuntimeException("Unable to load config", e);
                }
            }
        });
        reverseProxy.run();
        tunnelPortLatch.await();
        final Iterable<String> localAddresses = getAddress();
        String zkKey = Util.INSTANCE.getFullPath( Util.PREFIX
                                                , sessionName
                                                , new ConnectionString(getAddress()
                                                                      , tunnelPort.get()
                                                                      ).serialize()
                                                )
                     ;
        System.out.println("Initializing zknode " + zkKey);
        zkClient.create()
                .creatingParentsIfNeeded()
                .forPath(zkKey, State.INITIALIZE.toPayload());
        final NodeCache nc = new NodeCache(zkClient, zkKey);
        nc.getListenable().addListener(new NodeCacheListener()
        {

            /**
             * Called when a change has occurred
             */
            @Override
            public void nodeChanged() throws Exception {
                State currState = State.fromPayload(nc.getCurrentData().getData());
                if(currState == State.START_CLIENT)
                {
                    System.out.println("Starting client, but first have to wait for local port to be bound..");
                    localPortLatch.await();
                    runWorker(Iterables.getFirst(localAddresses, null), localPort.get(), dockerImage);
                }
                else if(currState == State.SHUTDOWN)
                {
                    //this is dumb and not safe, but it works.
                    //TODO: implement a sensible way for the proxy thread not die.
                    System.exit(0);
                }
            }
        }
        );
        nc.start();
        reverseProxy.join();
    }


    public static void runWorker(String outsideHostname, int localPort, String dockerImage)
    {
        String cmd = "/usr/lib64/R/bin/Rscript /usr/lib64/R/library/snow/RSOCKnode.R " +
                "MASTER=" + outsideHostname + " PORT=" + localPort +  " OUT=/dev/stdout " +
                "SNOWLIB=/usr/lib64/R/library TIMEOUT=10000";
        String dockedCmd = dockerImage != null?Joiner.on(' ')
                                                     .join("/usr/bin/docker run -i -t"
                                                             , dockerImage
                                                             , cmd
                                                     )
                                              :cmd;
        org.apache.commons.exec.CommandLine commandLine = org.apache.commons.exec.CommandLine.parse(dockedCmd);
        DefaultExecutor executor = new DefaultExecutor();
        //executor.setExitValue(1);
        System.out.println(dockedCmd);
        int exitValue = 0;
        try {
            exitValue = executor.execute(commandLine);
        } catch (IOException e) {
            throw new RuntimeException("Unable to run snow worker.  Exit of " + exitValue);
        }

        if(exitValue != 0)
        {
            throw new RuntimeException("Unable to run snow worker.  Exit of " + exitValue);
        }
    }


}

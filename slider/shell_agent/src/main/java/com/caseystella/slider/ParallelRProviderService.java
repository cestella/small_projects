package com.caseystella.slider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.server.appmaster.web.rest.agent.*;
import org.apache.slider.server.services.utility.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by cstella on 9/10/14.
 */
public class ParallelRProviderService extends AbstractProviderService
                                      implements ProviderCore
                                               , SliderKeys
                                               , AgentRestOperations
                                               , Keys
{
    protected static final Logger log =
            LoggerFactory.getLogger(ParallelRProviderService.class);
    private static final ProviderUtils providerUtils = new ProviderUtils(log);

    public ParallelRProviderService() {
        super("ParallelRProviderService");
        setAgentRestOperations(this);
    }

    @Override
    public RegistrationResponse handleRegistration(Register registration) {
        // dummy impl
        RegistrationResponse response = new RegistrationResponse();
        response.setResponseStatus(RegistrationStatus.OK);
        return response;
    }

    @Override
    public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {

        long id = heartBeat.getResponseId();
        HeartBeatResponse response = new HeartBeatResponse();
        response.setResponseId(id + 1L);
        return response;
    }

    /**
     * Set up the entire container launch context
     *
     * @param launcher
     * @param instanceDefinition
     * @param container
     * @param role
     * @param sliderFileSystem
     * @param generatedConfPath
     * @param resourceComponent
     * @param appComponent
     * @param containerTmpDirPath
     */
    @Override
    public void buildContainerLaunchContext(ContainerLauncher launcher
                                           , AggregateConf instanceDefinition
                                           , Container container
                                           , String role
                                           , SliderFileSystem sliderFileSystem
                                           , Path generatedConfPath
                                           , MapOperations resourceComponent
                                           , MapOperations appComponent
                                           , Path containerTmpDirPath
                                           )
    throws IOException, SliderException
    {

        // Set the environment
        launcher.putEnv(SliderUtils.buildEnvMap(appComponent));
        String command = appComponent.getOption(COMMAND, "");
        if(command.isEmpty())
        {
            throw new RuntimeException("Empty commands yield no fruit.");
        }
        String zk = appComponent.getOption(ZOOKEEPER, "");
        if(zk.isEmpty())
        {
            throw new RuntimeException("Must specify zookeeper quorum.");
        }

        CommandLineBuilder cli = new CommandLineBuilder();
        //this must stay relative if it is an image
        cli.add(providerUtils.buildPathToScript( instanceDefinition,
                                                 "bin",
                                                 command
                                               )
               );
        String configFile = appComponent.getOption(CONFIG, "");
        if(!configFile.isEmpty())
        {
            cli.add("-" + CONFIG);
            cli.add(configFile);
        }
        cli.add("-role");
        cli.add(role);
        cli.add("-" + ZOOKEEPER);
        cli.add(zk);
        cli.addOutAndErrFiles(providerUtils.getLogdir(), null);
        launcher.addCommand(cli.build());

    }

    /**
     * Execute a process in the AM
     *
     * @param instanceDefinition cluster description
     * @param confDir            configuration directory
     * @param env                environment
     * @param execInProgress     the callback for the exec events
     * @return true if a process was actually started
     * @throws java.io.IOException
     * @throws org.apache.slider.core.exceptions.SliderException
     */
    @Override
    public boolean exec(AggregateConf instanceDefinition, File confDir, Map<String, String> env, EventCallback execInProgress) throws IOException, SliderException {
        return false;
    }

    /**
     * Load a specific XML configuration file for the provider config
     *
     * @param confDir configuration directory
     * @return a configuration to be included in status
     * @throws org.apache.slider.core.exceptions.BadCommandArgumentsException
     * @throws java.io.IOException
     */
    @Override
    public Configuration loadProviderConfigurationInformation(File confDir) throws BadCommandArgumentsException, IOException {
        return new Configuration();
    }

    @Override
    public List<ProviderRole> getRoles() {
        return Roles.getRoles();
    }

    @Override
    public void validateInstanceDefinition(AggregateConf instanceDefinition) throws SliderException {

    }
}

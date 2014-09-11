package com.caseystella.slider;

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.providers.SliderProviderFactory;

/**
 * Created by cstella on 9/10/14.
 */
public class ShellAgentProviderFactory extends SliderProviderFactory {
    ShellAgentProviderFactory()
    {}
    ShellAgentProviderFactory(Configuration conf)
    {
        super(conf);
    }
    @Override
    public AbstractClientProvider createClientProvider() {
        return new ShellAgentClientProvider(getConf());
    }

    @Override
    public ProviderService createServerProvider() {
        return new ShellAgentProviderService();
    }
}

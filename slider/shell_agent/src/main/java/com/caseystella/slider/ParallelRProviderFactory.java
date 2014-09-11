package com.caseystella.slider;

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.providers.SliderProviderFactory;

/**
 * Created by cstella on 9/10/14.
 */
public class ParallelRProviderFactory extends SliderProviderFactory {
    ParallelRProviderFactory()
    {}
    ParallelRProviderFactory(Configuration conf)
    {
        super(conf);
    }
    @Override
    public AbstractClientProvider createClientProvider() {
        return null;
    }

    @Override
    public ProviderService createServerProvider() {
        return null;
    }
}

package com.caseystella.slider;

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.ProviderRole;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by cstella on 9/10/14.
 */
public class ParallelRClientProvider extends AbstractClientProvider implements SliderKeys {
    protected static final String NAME = "parallel_r";
    public ParallelRClientProvider(Configuration conf) {
        super(conf);
    }

    private static Set<String> knownRoleNames = new HashSet<>();
    static {
        List<ProviderRole> roles = Roles.getRoles();
        knownRoleNames.add(SliderKeys.COMPONENT_AM);
        for (ProviderRole role : roles) {
            knownRoleNames.add(role.name);
        }
    }
    /**
     * Validate the instance definition.
     * @param instanceDefinition instance definition
     */
    @Override
    public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
            SliderException {
        super.validateInstanceDefinition(instanceDefinition);
        ConfTreeOperations resources =
                instanceDefinition.getResourceOperations();
        Set<String> unknownRoles = resources.getComponentNames();
        unknownRoles.removeAll(knownRoleNames);
        if (!unknownRoles.isEmpty()) {
            throw new BadCommandArgumentsException("Unknown component: %s",
                    unknownRoles.iterator().next());
        }
        providerUtils.validateNodeCount(instanceDefinition, Roles.ROLE_WORKER,
                0, -1);
        providerUtils.validateNodeCount(instanceDefinition, Roles.ROLE_MASTER,
                0, -1);

    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<ProviderRole> getRoles() {
        return Roles.getRoles();
    }
}

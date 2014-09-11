package com.caseystella.slider;

import org.apache.slider.common.SliderKeys;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cstella on 9/10/14.
 */
public class Roles {



    /**
     * List of roles
     */
    protected static final List<ProviderRole> ROLES =
            new ArrayList<ProviderRole>();

    public static final String ROLE_WORKER = "worker";
    public static final String ROLE_MASTER= "master";
    public static final int KEY_WORKER = SliderKeys.ROLE_AM_PRIORITY_INDEX + 1;

    public static final int KEY_MASTER = SliderKeys.ROLE_AM_PRIORITY_INDEX + 2;

    /**
     * Initialize role list
     */
    static {
        ROLES.add(new ProviderRole(ROLE_WORKER, KEY_WORKER, PlacementPolicy.NO_DATA_LOCALITY));
        // Master doesn't need data locality
        ROLES.add(new ProviderRole(ROLE_MASTER, KEY_MASTER, PlacementPolicy.NO_DATA_LOCALITY));
    }


    public static List<ProviderRole> getRoles() {
        return ROLES;
    }
}

package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfig {
    private String databases;
    private String backup;
    private OConfigSecurity security;
    private List<OConfigNetwork> network;
    private List<OConfigPlugin> plugin;
    private OConfigDistributed distributed;
}

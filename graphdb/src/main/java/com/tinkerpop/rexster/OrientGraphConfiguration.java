package com.tinkerpop.rexster;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationContext;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

/**
 * Configuration class for Rexster (http://rexster.tinkerpop.com).  Example usage within rexster.xml:
 * <br/>
 * <pre>
 * {@code
 * <graph>
 *   <graph-enabled>false</graph-enabled>
 *   <graph-name>orientdbsample</graph-name>
 *   <graph-type>com.tinkerpop.rexster.OrientGraphConfiguration</graph-type>
 *   <graph-location>local:/tmp/orientdb-graph</graph-location>
 *   <properties>
 *     <username>admin</username>
 *     <password>admin</password>
 *   </properties>
 *   <extensions>
 *     <allows>
 *       <allow>tp:gremlin</allow>
 *     </allows>
 *   </extensions>
 * </graph>
 * }
 * </pre>
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OrientGraphConfiguration implements GraphConfiguration {

    public Graph configureGraphInstance(final GraphConfigurationContext context) throws GraphConfigurationException {

        final String graphFile = context.getProperties().getString(Tokens.REXSTER_GRAPH_LOCATION);

        if (graphFile == null || graphFile.length() == 0) {
            throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_LOCATION);
        }

        // get the <properties> section of the xml configuration
        final HierarchicalConfiguration graphSectionConfig = (HierarchicalConfiguration) context.getProperties();
        SubnodeConfiguration orientDbSpecificConfiguration;

        try {
            orientDbSpecificConfiguration = graphSectionConfig.configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
        } catch (IllegalArgumentException iae) {
            throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_PROPERTIES);
        }

        try {

            final String username = orientDbSpecificConfiguration.getString("username", "");
            final String password = orientDbSpecificConfiguration.getString("password", "");

            // Caching must be turned off. OrientDB has different layers of cache:
            // http://code.google.com/p/orient/wiki/Caching There's one Level1 cache per OGraphDatabase instance
            // and one level2 per JVM. If a OGraphDatabase caches a vertex and then you change it in
            // another thread/transaction you could see the older one. To fix it just disable the Level1 cache.
            // If there were multiple running JVM you could have Level2 cache not updated for the same reason as
            // above. Then you've to disable Level2 cache....per Luca.
            //
            // Disabling the level 1 cache seems to solve the problem where POSTs of edges in rapid succession
            // force a transaction error like: Cannot update record #6:0 in storage 'orientdb-graph' because the
            // version is not the latest. Probably you are updating an old record or it has been modified by
            // another user (db=v2 your=v0)
            OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);

            // calling the open method opens the connection to graphdb.  looks like the
            // implementation of shutdown will call the orientdb close method.
            return new OrientGraph(graphFile, username, password);

        } catch (Exception ex) {
            throw new GraphConfigurationException(ex);
        }
    }

}

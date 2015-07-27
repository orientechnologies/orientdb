/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.tinkerpop.rexster;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationContext;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

/**
 * Configuration class for Rexster (http://rexster.tinkerpop.com). Example usage within rexster.xml: <br>
 * 
 * <pre>
 * {@code
 * <graph>
 *   <graph-enabled>false</graph-enabled>
 *   <graph-name>orientdbsample</graph-name>
 *   <graph-type>com.tinkerpop.rexster.OrientGraphConfiguration</graph-type>
 *   <graph-location>plocal:/tmp/orientdb-graph</graph-location>
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
 * 
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OrientGraphConfiguration implements GraphConfiguration {

  public Graph configureGraphInstance(final GraphConfigurationContext context) throws GraphConfigurationException {

    final String graphFile = context.getProperties().getString(Tokens.REXSTER_GRAPH_LOCATION);

    if (graphFile == null || graphFile.length() == 0) {
      throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: "
          + Tokens.REXSTER_GRAPH_LOCATION);
    }

    // get the <properties> section of the xml configuration
    final HierarchicalConfiguration graphSectionConfig = (HierarchicalConfiguration) context.getProperties();
    SubnodeConfiguration orientDbSpecificConfiguration;

    try {
      orientDbSpecificConfiguration = graphSectionConfig.configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
    } catch (IllegalArgumentException iae) {
      throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: "
          + Tokens.REXSTER_GRAPH_PROPERTIES, iae);
    }

    try {

      final String username = orientDbSpecificConfiguration.getString("username", "");
      final String password = orientDbSpecificConfiguration.getString("password", "");


      // calling the open method opens the connection to graphdb. looks like the
      // implementation of shutdown will call the orientdb close method.
      return new OrientGraph(graphFile, username, password);

    } catch (Exception ex) {
      throw new GraphConfigurationException(ex);
    }
  }

}

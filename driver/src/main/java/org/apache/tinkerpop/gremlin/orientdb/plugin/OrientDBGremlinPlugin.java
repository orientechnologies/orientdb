package org.apache.tinkerpop.gremlin.orientdb.plugin;

import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Enrico Risa on 06/02/17.
 */
public class OrientDBGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.orientdb";

    private static final Set<String> IMPORTS = new HashSet<String>() {
        {
            add(IMPORT_SPACE + OrientGraph.class.getPackage().getName() + DOT_STAR);
        }
    };

    @Override
    public void pluginTo(PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }

    @Override
    public String getName() {
        return NAME;
    }
}

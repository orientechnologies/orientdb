package org.apache.tinkerpop.gremlin.orientdb.plugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;

/**
 * Created by Enrico Risa on 06/02/17.
 */
public class OrientDBGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "tinkerpop.orientdb";

    public OrientDBGremlinPlugin() {
        super(NAME, null);
    }

    @Override
    public String getName() {
        return NAME;
    }
}

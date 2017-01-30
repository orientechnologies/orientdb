package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import org.apache.tinkerpop.gremlin.orientdb.OrientProperty;

/**
 * Created by Enrico Risa on 24/01/17.
 */
public class OrientPropertyTransformer implements OResultTransformer<OrientProperty> {

    @Override
    public OResult transform(OrientProperty element) {

        OResultInternal internal = new OResultInternal();
        internal.setProperty(element.key(), element.value());
        return internal;
    }
}

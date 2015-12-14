package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Iterator;

public class OrientVertexIterator extends OLazyWrapperIterator<Vertex> {
    private final OrientVertex vertex;
    private final String[] iLabels;
    private final OPair<Direction, String> connection;

    public OrientVertexIterator(final OrientVertex orientVertex, final Object iMultiValue, final Iterator<?> iterator,
            final OPair<Direction, String> connection, final String[] iLabels, final int iSize) {
        super(iterator, iSize, iMultiValue);
        this.vertex = orientVertex;
        this.connection = connection;
        this.iLabels = iLabels;
    }

    @Override
    public Vertex createGraphElement(final Object iObject) {
        if (iObject instanceof OrientVertex)
            return (OrientVertex) iObject;

        if (iObject == null) {
            return null;
        }

        final ORecord rec = ((OIdentifiable) iObject).getRecord();

        if (rec == null || !(rec instanceof ODocument))
            return null;

        final ODocument value = (ODocument) rec;

        final OrientVertex v;
        OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(value);
        if (immutableClass.isVertexType()) {
            // DIRECT VERTEX
            v = new OrientVertex(vertex.getGraph(), value);
        } else if (immutableClass.isEdgeType()) {
            // EDGE
            // if (vertex.settings.isUseVertexFieldsForEdgeLabels() ||
            //            if (vertex.settings.isUseVertexFieldsForEdgeLabels() || OrientEdge.isLabeled(OrientEdge.getRecordLabel(value), iLabels))
            v = new OrientVertex(vertex.getGraph(), OrientEdge.getConnection(value, connection.getKey().opposite()));
            //            else
            //                v = null;
        } else
            throw new IllegalStateException("Invalid content found between connections:" + value);

        return v;
    }

    @Override
    public OIdentifiable getGraphElementRecord(final Object iObject) {
        final ORecord rec = ((OIdentifiable) iObject).getRecord();

        if (rec == null || !(rec instanceof ODocument))
            return null;

        final ODocument value = (ODocument) rec;

        final OIdentifiable v;
        OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(value);
        if (immutableClass.isVertexType()) {
            // DIRECT VERTEX
            v = value;
        } else if (immutableClass.isEdgeType()) {
            // EDGE
            //            if (vertex.settings.isUseVertexFieldsForEdgeLabels() || OrientEdge.isLabeled(OrientEdge.getRecordLabel(value), iLabels))
            v = OrientEdge.getConnection(value, connection.getKey().opposite());
            //            else
            //                v = null;
        } else
            throw new IllegalStateException("Invalid content found between connections:" + value);

        return v;
    }

    public boolean filter(final Vertex iObject) {
        if (iObject instanceof OrientVertex && ((OrientVertex) iObject).getRawElement() == null) {
            return false;
        }
        return true;
    }

    private boolean isVertex(final OIdentifiable iObject) {
        final ORecord rec = iObject.getRecord();

        if (rec == null || !(rec instanceof ODocument))
            return false;

        final ODocument value = (ODocument) rec;

        final OIdentifiable v;
        OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(value);
        if (immutableClass.isVertexType()) {
            // DIRECT VERTEX
            return true;
        } else if (immutableClass.isEdgeType()) {
            return false;
        }

        throw new IllegalStateException("Invalid content found between connections: " + value);
    }

    @Override
    public boolean canUseMultiValueDirectly() {
        if (multiValue instanceof Collection) {
            if ((((Collection) multiValue).isEmpty()) || isVertex((OIdentifiable) ((Collection) multiValue).iterator().next()))
                return true;
        } else if (multiValue instanceof ORidBag) {
            if ((((ORidBag) multiValue).isEmpty()) || isVertex(((ORidBag) multiValue).iterator().next()))
                return true;
        }

        return false;
    }
}

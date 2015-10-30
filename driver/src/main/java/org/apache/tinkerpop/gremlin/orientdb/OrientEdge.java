package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class OrientEdge extends OrientElement implements Edge {
	
	private static final List<String> INTERNAL_FIELDS = Arrays.asList("@rid", "@class", "in", "out");
	
    protected OIdentifiable   vOut;
    protected OIdentifiable   vIn;
    protected String          label;

    public OrientEdge(OrientGraph graph, OIdentifiable rawElement, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
        super(graph, rawElement);
        vOut = checkNotNull(out);
        vIn = checkNotNull(in);
        label = checkNotNull(iLabel);
    }

    public OrientEdge(OrientGraph graph, String className, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
        this(graph, createRawElement(graph, className), out, in, iLabel);
    }

    public OrientEdge(OrientGraph graph, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
      this(graph, (OIdentifiable) null, out, in, iLabel);
    }

    public OrientEdge(OrientGraph graph, ODocument rawDocument, String label) {
        this(graph, rawDocument, rawDocument.field("out", OIdentifiable.class), rawDocument.field("in", OIdentifiable.class), label);
    }

    public OrientEdge(OrientGraph graph, ODocument rawDocument)
    {
      this(graph, rawDocument, rawDocument.getClassName());
    }

    public static OIdentifiable getConnection(final ODocument iEdgeRecord, final Direction iDirection) {
        return iEdgeRecord.rawField(iDirection == Direction.OUT ? OrientGraphUtils.CONNECTION_OUT : OrientGraphUtils.CONNECTION_IN);
    }

    protected static ODocument createRawElement(OrientGraph graph, String className) {
        graph.createEdgeClass(className);
        return new ODocument(className);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
      switch (direction)
      {
      case OUT:
        return graph.vertices(vOut.getIdentity());
      case IN:
        return graph.vertices(vIn.getIdentity());
      case BOTH:
      default:
        return graph.vertices(vOut.getIdentity(), vIn.getIdentity());
      }
    }

    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
        return StreamUtils.asStream(properties).filter(p ->
        	!INTERNAL_FIELDS.contains(p.key()) ).map(p ->
            (Property<V>) p
        ).iterator();
    }

    public OrientVertex getVertex(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return new OrientVertex(graph, getOutVertex());
        else if (direction.equals(Direction.IN))
            return new OrientVertex(graph, getInVertex());
        else
            throw new IllegalArgumentException("direction " + direction + " is not supported!");
    }

    public OIdentifiable getOutVertex() {
        if (vOut != null)
            // LIGHTWEIGHT EDGE
            return vOut;

        final ODocument doc = getRawDocument();
        if (doc == null)
            return null;

//        if (settings != null && settings.isKeepInMemoryReferences())
            // AVOID LAZY RESOLVING+SETTING OF RECORD
//            return doc.rawField(OrientGraphUtils.CONNECTION_OUT);
//        else
            return doc.field(OrientGraphUtils.CONNECTION_OUT);
    }

    /**
     * (Blueprints Extension) Returns the incoming vertex in form of record.
     */
    public OIdentifiable getInVertex() {
        if (vIn != null)
            // LIGHTWEIGHT EDGE
            return vIn;

        final ODocument doc = getRawDocument();
        System.out.println(doc);
        System.out.println(Arrays.asList(doc.fieldNames()));

        if (doc == null)
            return null;

//        if (settings != null && settings.isKeepInMemoryReferences())
            // AVOID LAZY RESOLVING+SETTING OF RECORD
//            return doc.rawField(OrientGraphUtils.CONNECTION_IN);
//        else
            return doc.field(OrientGraphUtils.CONNECTION_IN);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

	@Override
	public int hashCode() {
		final int prime = 73;
		int result = super.hashCode();
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		result = prime * result + ((vIn == null) ? 0 : vIn.hashCode());
		result = prime * result + ((vOut == null) ? 0 : vOut.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OrientEdge other = (OrientEdge) obj;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		if (vIn == null) {
			if (other.vIn != null)
				return false;
		} else if (!vIn.equals(other.vIn))
			return false;
		if (vOut == null) {
			if (other.vOut != null)
				return false;
		} else if (!vOut.equals(other.vOut))
			return false;
		return true;
	}
}

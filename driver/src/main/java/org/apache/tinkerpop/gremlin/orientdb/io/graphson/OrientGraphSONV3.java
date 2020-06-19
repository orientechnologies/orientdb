package org.apache.tinkerpop.gremlin.orientdb.io.graphson;

import static org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry.isORecord;
import static org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry.newORecordId;

import com.orientechnologies.orient.core.id.ORecordId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;

/** Created by Enrico Risa on 06/09/2017. */
public class OrientGraphSONV3 extends OrientGraphSON {

  public static final OrientGraphSONV3 INSTANCE = new OrientGraphSONV3();

  protected static final Map<Class, String> TYPES =
      Collections.unmodifiableMap(
          new LinkedHashMap<Class, String>() {
            {
              put(ORecordId.class, "ORecordId");
            }
          });

  public OrientGraphSONV3() {
    super("orient-graphson-v3");
    addSerializer(ORecordId.class, new ORecordIdJacksonSerializer());
    addDeserializer(ORecordId.class, new ORecordIdJacksonDeserializer());

    addDeserializer(Edge.class, new EdgeJacksonDeserializer());
    addDeserializer(Vertex.class, new VertexJacksonDeserializer());
    addDeserializer(Map.class, (JsonDeserializer) new ORecordIdDeserializer());
  }

  @Override
  public Map<Class, String> getTypeDefinitions() {
    return TYPES;
  }

  /** Created by Enrico Risa on 06/09/2017. */
  public static class EdgeJacksonDeserializer extends AbstractObjectDeserializer<Edge> {

    public EdgeJacksonDeserializer() {
      super(Edge.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Edge createObject(final Map<String, Object> edgeData) {
      return new DetachedEdge(
          newORecordId(edgeData.get(GraphSONTokens.ID)),
          edgeData.get(GraphSONTokens.LABEL).toString(),
          (Map) edgeData.get(GraphSONTokens.PROPERTIES),
          newORecordId(edgeData.get(GraphSONTokens.OUT)),
          edgeData.get(GraphSONTokens.OUT_LABEL).toString(),
          newORecordId(edgeData.get(GraphSONTokens.IN)),
          edgeData.get(GraphSONTokens.IN_LABEL).toString());
    }
  }

  /** Created by Enrico Risa on 06/09/2017. */
  public static class VertexJacksonDeserializer extends AbstractObjectDeserializer<Vertex> {

    public VertexJacksonDeserializer() {
      super(Vertex.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Vertex createObject(final Map<String, Object> vertexData) {
      return new DetachedVertex(
          newORecordId(vertexData.get(GraphSONTokens.ID)),
          vertexData.get(GraphSONTokens.LABEL).toString(),
          (Map<String, Object>) vertexData.get(GraphSONTokens.PROPERTIES));
    }
  }

  static final class ORecordIdDeserializer extends AbstractObjectDeserializer<Object> {

    public ORecordIdDeserializer() {
      super(Object.class);
    }

    @Override
    public Object createObject(Map<String, Object> data) {

      if (isORecord(data)) {
        return newORecordId(data);
      }
      return data;
    }
  }
}

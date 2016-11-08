package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.UntypedObjectDeserializer.Vanilla;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Map;

@SuppressWarnings("serial")
public class OrientIoRegistry extends AbstractIoRegistry {

    private static final String CLUSTER_ID = "clusterId";
    private static final String CLUSTER_POSITION = "clusterPosition";

    private static final OrientIoRegistry INSTANCE = new OrientIoRegistry();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private OrientIoRegistry() {
        register(GryoIo.class, ORecordId.class, new ORecordIdKyroSerializer());
        register(GryoIo.class, ORidBag.class, new ORidBagKyroSerializer());
        SimpleModule serializer = new SimpleModule();
        serializer.addSerializer(ORecordId.class, new ORecordIdJacksonSerializer());
        serializer.addDeserializer(ORecordId.class, (JsonDeserializer) new ORecordIdDeserializer());
        serializer.addSerializer(ORidBag.class, new ORidBagJacksonSerializer());
        serializer.addDeserializer(ORidBag.class, (JsonDeserializer) new ORidBagDeserializer());
        serializer.addDeserializer(Object.class, new OObjectJacksonDeserializer());

        serializer.addDeserializer(Edge.class, new EdgeJacksonDeserializer());
        serializer.addDeserializer(Vertex.class, new VertexJacksonDeserializer());
        serializer.addDeserializer(Map.class, (JsonDeserializer) new ORecordIdDeserializer());

        register(GraphSONIo.class, ORecordId.class, serializer);
        register(GraphSONIo.class, Map.class, serializer);
    }

    public static OrientIoRegistry getInstance() {
        return INSTANCE;
    }

    private static ORecordId newORecordId(Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof ORecordId)
            return (ORecordId) obj;

        if (obj instanceof Map) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Map<String, Number> map = (Map) obj;
            return new ORecordId(map.get(CLUSTER_ID).intValue(), map.get(CLUSTER_POSITION).longValue());
        }

        throw new IllegalArgumentException("Unable to convert unknow type to ORecordId " + obj.getClass());
    }

    private static boolean isORecord(Object result) {
        if (!(result instanceof Map))
            return false;

        @SuppressWarnings("unchecked")
        Map<String, Number> map = (Map<String, Number>) result;
        return map.containsKey(CLUSTER_ID) && map.containsKey(CLUSTER_POSITION);
    }

    final static class OObjectJacksonDeserializer extends Vanilla {

        @Override
        public Object deserializeWithType(JsonParser jp, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return super.deserializeWithType(jp, ctxt, typeDeserializer);
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            Object result = super.deserialize(p, ctxt);
            if (result instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Number> map = (Map<String, Number>) result;
                if (map.containsKey(CLUSTER_ID) && map.containsKey(CLUSTER_POSITION)) {
                    return new ORecordId(map.get(CLUSTER_ID).intValue(), map.get(CLUSTER_POSITION).longValue());
                }
            }
            return result;
        }

    }

    final static class ORecordIdJacksonSerializer extends JsonSerializer<ORecordId> {

        @Override
        public void serialize(ORecordId value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            this.serializeWithType(value, jgen, provider, null);
        }

        @Override
        public void serializeWithType(ORecordId value, JsonGenerator jgen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            if (typeSer != null) {
                typeSer.writeTypePrefixForObject(value, jgen);
            } else
                jgen.writeStartObject();
            jgen.writeFieldName(CLUSTER_ID);
            jgen.writeNumber(value.getClusterId());
            jgen.writeFieldName(CLUSTER_POSITION);
            jgen.writeNumber(value.getClusterPosition());
            if (typeSer != null) {
                typeSer.writeTypeSuffixForObject(value, jgen);
            } else
                jgen.writeEndObject();
        }

    }

    final static class ORidBagJacksonSerializer extends JsonSerializer<ORidBag> {

        @Override
        public void serialize(ORidBag value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            this.serializeWithType(value, jgen, provider, null);
        }

        @Override
        public void serializeWithType(ORidBag value, JsonGenerator jgen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            jgen.writeStartArray();
            if (typeSer != null)
                jgen.writeStringField(GraphSONTokens.CLASS, ORidBag.class.getName());
            for (OIdentifiable id : value)
                jgen.writeString(id.getIdentity().toString());

            jgen.writeEndArray();
        }

    }

    final static class ORidBagDeserializer extends Vanilla {

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            Object result = super.deserialize(p, ctxt);
            if (result instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Number> map = (Map<String, Number>) result;
                if (map.containsKey(CLUSTER_ID) && map.containsKey(CLUSTER_POSITION)) {
                    return new ORecordId(map.get(CLUSTER_ID).intValue(), map.get(CLUSTER_POSITION).longValue());
                }
            }
            return result;
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return super.deserializeWithType(p, ctxt, typeDeserializer);
        }
    }

    final static class ORecordIdDeserializer extends Vanilla {

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            Object result = super.deserialize(p, ctxt);
            if (isORecord(result)) {
                return newORecordId(result);
            }
            return result;
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return super.deserializeWithType(p, ctxt, typeDeserializer);
        }
    }

    final static class ORecordIdKyroSerializer extends Serializer<ORecordId> {

        @Override
        public ORecordId read(final Kryo kryo, final Input input, final Class<ORecordId> tinkerGraphClass) {
            return new ORecordId(input.readString());
        }

        @Override
        public void write(final Kryo kryo, final Output output, final ORecordId rid) {
            output.writeString(rid.toString());
        }

    }

    final static class ORidBagKyroSerializer extends Serializer<ORidBag> {

        @Override
        public ORidBag read(final Kryo kryo, final Input input, final Class<ORidBag> tinkerGraphClass) {
            ORidBag bag = new ORidBag();
            String[] ids = input.readString().split(";");

            for (String id : ids)
                bag.add(new ORecordId(id));
            return bag;
        }

        @Override
        public void write(final Kryo kryo, final Output output, final ORidBag bag) {
            StringBuilder ids = new StringBuilder();
            bag.forEach(rid -> ids.append(rid.getIdentity()).append(";"));
            output.writeString(ids);
        }

    }

    static class EdgeJacksonDeserializer extends AbstractObjectDeserializer<Edge> {

        public EdgeJacksonDeserializer() {
            super(Edge.class);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public Edge createObject(final Map<String, Object> edgeData) {
            return new DetachedEdge(
                    newORecordId(edgeData.get(GraphSONTokens.ID)),
                    edgeData.get(GraphSONTokens.LABEL).toString(),
                    (Map) edgeData.get(GraphSONTokens.PROPERTIES),
                    Pair.with(newORecordId(edgeData.get(GraphSONTokens.OUT)), edgeData.get(GraphSONTokens.OUT_LABEL).toString()),
                    Pair.with(newORecordId(edgeData.get(GraphSONTokens.IN)), edgeData.get(GraphSONTokens.IN_LABEL).toString()));
        }
    }

    static class VertexJacksonDeserializer extends AbstractObjectDeserializer<Vertex> {

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

}

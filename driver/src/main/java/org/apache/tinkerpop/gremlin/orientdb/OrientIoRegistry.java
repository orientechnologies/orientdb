package org.apache.tinkerpop.gremlin.orientdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer.Vanilla;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.orientechnologies.orient.core.id.ORecordId;

@SuppressWarnings("serial")
public class OrientIoRegistry extends AbstractIoRegistry {

    private static final OrientIoRegistry INSTANCE = new OrientIoRegistry();

    private OrientIoRegistry() {
        register(GryoIo.class, ORecordId.class, new ORecordIdKyroSerializer());
        SimpleModule serializer = new SimpleModule();
        serializer.addSerializer(ORecordId.class, new ORecordIdJacksonSerializer());
        serializer.addDeserializer(Object.class, new OObjectJacksonDeserializer());
        register(GraphSONIo.class, ORecordId.class, serializer);
        register(GraphSONIo.class, Map.class, serializer);
    }

    public static OrientIoRegistry getInstance() {
        return INSTANCE;
    }

    final static class OObjectJacksonDeserializer extends Vanilla {

        @Override
        public Object deserializeWithType(JsonParser jp, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            switch (jp.getCurrentTokenId()) {
            case JsonTokenId.ID_START_OBJECT:
                return deserialize(jp, ctxt);
            }
            Object result = super.deserializeWithType(jp, ctxt, typeDeserializer);
            return result;
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            Object result = super.deserialize(p, ctxt);
            if (result instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Number> map = (Map<String, Number>) result;
                if (map.containsKey("clusterId") && map.containsKey("clusterPosition")) {
                    return new ORecordId(map.get("clusterId").intValue(), map.get("clusterPosition").longValue());
                }
            }
            return result;
        }

    }

    final static class OMapJacksonDeserializer extends StdDeserializer<Map<String, Object>> {

        private static final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {
        };

        public OMapJacksonDeserializer() {
            super(TypeFactory.defaultInstance().constructType(mapTypeReference));
        }

        @Override
        public Object deserializeWithType(JsonParser jp, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return deserialize(jp, ctxt);
        }

        @Override
        public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            ObjectCodec codec = p.getCodec();
            TreeNode tree = codec.readTree(p);
            if (!tree.isObject())
                throw new JsonMappingException("Upexpected error");

            Map<String, Object> result = nodeToMap((JsonNode) tree);
            return result;
        }

        private Map<String, Object> nodeToMap(JsonNode node) {
            Map<String, Object> result = new HashMap<>();
            node.fields().forEachRemaining(entry -> {
                JsonNode entryValue = entry.getValue();
                Object value;
                if (entryValue.isObject()) {
                    if (entryValue.has("clusterId") && entryValue.has("clusterPosition")) {
                        value = new ORecordId(entryValue.get("clusterId").asInt(), entryValue.get("clusterPosition").asLong());
                    } else {
                        value = nodeToMap(entryValue);
                    }
                } else {
                    if (entryValue.isInt())
                        value = entryValue.asInt();
                    else if (entryValue.isBoolean())
                        value = entryValue.asBoolean();
                    else if (entryValue.isDouble())
                        value = entryValue.asDouble();
                    else
                        value = entryValue.asText();
                }
                result.put(entry.getKey(), value);
            });
            return result;
        }

    }

    final static class ORecordIdJacksonDeserializer extends StdDeserializer<ORecordId> {

        protected ORecordIdJacksonDeserializer() {
            super(ORecordId.class);
        }

        @Override
        public ORecordId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            ObjectCodec codec = p.getCodec();
            TreeNode tree = codec.readTree(p);
            return new ORecordId(((NumericNode) tree.get("clusterId")).asInt(), ((NumericNode) tree.get("clusterPosition")).asLong());
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return deserialize(p, ctxt);
        }

    }

    final static class ORecordIdJacksonSerializer extends JsonSerializer<ORecordId> {

        @Override
        public void serialize(ORecordId value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeObjectField("clusterId", value.clusterId);
            jgen.writeObjectField("clusterPosition", value.clusterPosition);
            jgen.writeEndObject();
        }

        @Override
        public void serializeWithType(ORecordId value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            serialize(value, gen, serializers);
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

}

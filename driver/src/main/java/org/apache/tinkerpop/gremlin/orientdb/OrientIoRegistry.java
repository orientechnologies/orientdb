package org.apache.tinkerpop.gremlin.orientdb;

import java.io.IOException;
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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer.Vanilla;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;

@SuppressWarnings("serial")
public class OrientIoRegistry extends AbstractIoRegistry {

    private static final String CLUSTER_ID = "clusterId";
    private static final String CLUSTER_POSITION = "clusterPosition";

    private static final OrientIoRegistry INSTANCE = new OrientIoRegistry();

    private OrientIoRegistry() {
        register(GryoIo.class, ORecordId.class, new ORecordIdKyroSerializer());
        register(GryoIo.class, ORidBag.class, new ORidBagKyroSerializer());
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
            jgen.writeStartObject();
            jgen.writeFieldName(CLUSTER_ID);
            jgen.writeNumber(value.clusterId);
            jgen.writeFieldName(CLUSTER_POSITION);
            jgen.writeNumber(value.clusterPosition);
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

}

package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers.*;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class SerializerFactory {
    public static final SerializerFactory INSTANCE = new SerializerFactory();
    private static final EnumMap<OType, ValueSerializer> TYPE_SERIALIZERS = new EnumMap<>(OType.class);
    private static final HashMap<String, OType> ID_TYPE_MAP = new HashMap<>();

    static {
        TYPE_SERIALIZERS.put(OType.BINARY, new BinarySerializer());
        TYPE_SERIALIZERS.put(OType.BYTE, new ByteSerializer());
        TYPE_SERIALIZERS.put(OType.CUSTOM, new CustomSerializer());
        TYPE_SERIALIZERS.put(OType.DATE, new DateSerializer());
        TYPE_SERIALIZERS.put(OType.DATETIME, new DateTimeSerializer());
        TYPE_SERIALIZERS.put(OType.DECIMAL, new DecimalSerializer());
        TYPE_SERIALIZERS.put(OType.EMBEDDEDLIST, new EmbeddedListSerializer());
        TYPE_SERIALIZERS.put(OType.FLOAT, new FloatSerializer());
        TYPE_SERIALIZERS.put(OType.LINKBAG, new LinkBagSerializer());
        TYPE_SERIALIZERS.put(OType.LINKLIST, new LinkListSerializer());
        TYPE_SERIALIZERS.put(OType.LINKMAP, new LinkMapSerializer());
        TYPE_SERIALIZERS.put(OType.LINK, new LinkSerializer());
        TYPE_SERIALIZERS.put(OType.LONG, new LongSerializer());
        TYPE_SERIALIZERS.put(OType.EMBEDDEDSET, new EmbeddedSetSerializer());
        TYPE_SERIALIZERS.put(OType.SHORT, new ShortSerializer());
        TYPE_SERIALIZERS.put(OType.EMBEDDEDMAP, new EmbeddedMapSerializer());

        for (Map.Entry<OType, ValueSerializer> entry : TYPE_SERIALIZERS.entrySet()) {
            final OType prevType = ID_TYPE_MAP.put(entry.getValue().typeId(), entry.getKey());

            if (prevType != null) {
                throw new IllegalStateException("Type id - '" + entry.getValue().typeId()
                        + "' has been already associated with the type " + prevType);
            }
        }
    }


    public ValueSerializer findSerializer(final OType fieldType) {
        Objects.requireNonNull(fieldType);

        final ValueSerializer serializer = TYPE_SERIALIZERS.get(fieldType);
        if (serializer == null) {
            throw new ODatabaseException("Provided type " + fieldType + " can not be serialized into JSON");
        }

        return serializer;
    }

    public String fieldTypeId(final OType type) {
        return TYPE_SERIALIZERS.get(type).typeId();
    }
}

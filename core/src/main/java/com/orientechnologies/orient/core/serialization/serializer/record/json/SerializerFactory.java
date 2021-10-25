package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers.*;

import java.util.*;

public final class SerializerFactory {
  public static final SerializerFactory INSTANCE = new SerializerFactory();
  private static final EnumMap<OType, ValueSerializer> TYPE_SERIALIZERS =
      new EnumMap<>(OType.class);
  private static final HashMap<String, OType> ID_TYPE_MAP = new HashMap<>();

  static {
    TYPE_SERIALIZERS.put(OType.BINARY, new BinarySerializer());
    TYPE_SERIALIZERS.put(OType.BYTE, new ByteSerializer());
    TYPE_SERIALIZERS.put(OType.BOOLEAN, new BooleanSerializer());
    TYPE_SERIALIZERS.put(OType.CUSTOM, new CustomSerializer());
    TYPE_SERIALIZERS.put(OType.DATE, new DateSerializer());
    TYPE_SERIALIZERS.put(OType.DATETIME, new DateTimeSerializer());
    TYPE_SERIALIZERS.put(OType.DECIMAL, new DecimalSerializer());
    TYPE_SERIALIZERS.put(OType.EMBEDDEDLIST, new EmbeddedListSerializer());
    TYPE_SERIALIZERS.put(OType.FLOAT, new FloatSerializer());
    TYPE_SERIALIZERS.put(OType.DOUBLE, new DoubleSerializer());
    TYPE_SERIALIZERS.put(OType.LINKBAG, new LinkBagSerializer());
    TYPE_SERIALIZERS.put(OType.LINKLIST, new LinkListSerializer());
    TYPE_SERIALIZERS.put(OType.LINKSET, new LinkSetSerializer());
    TYPE_SERIALIZERS.put(OType.LINKMAP, new LinkMapSerializer());
    TYPE_SERIALIZERS.put(OType.LINK, new LinkSerializer());
    TYPE_SERIALIZERS.put(OType.INTEGER, new IntegerSerializer());
    TYPE_SERIALIZERS.put(OType.LONG, new LongSerializer());
    TYPE_SERIALIZERS.put(OType.EMBEDDEDSET, new EmbeddedSetSerializer());
    TYPE_SERIALIZERS.put(OType.SHORT, new ShortSerializer());
    TYPE_SERIALIZERS.put(OType.EMBEDDEDMAP, new EmbeddedMapSerializer());
    TYPE_SERIALIZERS.put(OType.STRING, new StringSerializer());
    TYPE_SERIALIZERS.put(OType.EMBEDDED, DocumentSerializer.INSTANCE);

    for (Map.Entry<OType, ValueSerializer> entry : TYPE_SERIALIZERS.entrySet()) {
      final OType prevType = ID_TYPE_MAP.put(entry.getValue().typeId(), entry.getKey());

      if (prevType != null) {
        throw new IllegalStateException(
            "Type id - '"
                + entry.getValue().typeId()
                + "' has been already associated with the type "
                + prevType);
      }
    }
  }

  public ValueSerializer findSerializer(final OType fieldType) {
    Objects.requireNonNull(fieldType);

    final ValueSerializer serializer = TYPE_SERIALIZERS.get(fieldType);
    if (serializer == null) {
      throw new ODatabaseException(
          "Provided type " + fieldType + " can not be serialized into JSON");
    }

    return serializer;
  }

  public ValueSerializer findSerializer(final OType fieldType, final JsonToken token) {
    Objects.requireNonNull(fieldType);
    Objects.requireNonNull(token);
    final String errorMessage = "Invalid token %s for field type %s, expected %s";

    final ValueSerializer serializer = findSerializer(fieldType);
    final JsonToken[] expectedJsonTokens = serializer.startTokens();
    if (!containsToken(expectedJsonTokens, token)) {
      throw new ODatabaseException(
          String.format(errorMessage, token, fieldType, Arrays.toString(expectedJsonTokens)));
    }

    return serializer;
  }

  private static boolean containsToken(final JsonToken[] tokens, JsonToken token) {
    for (final JsonToken tk : tokens) {
      if (tk == token) {
        return true;
      }
    }

    return false;
  }

  public ValueSerializer findSerializer(final JsonToken token) {
    if (token == JsonToken.VALUE_NULL) {
      return null;
    }

    if (token.isNumeric()) {
      return findSerializer(OType.LONG);
    }
    if (token.isBoolean()) {
      return findSerializer(OType.BOOLEAN);
    }

    if (token == JsonToken.START_OBJECT) {
      return DocumentSerializer.INSTANCE;
    }

    if (token == JsonToken.START_ARRAY) {
      return findSerializer(OType.EMBEDDEDLIST);
    }

    if (token == JsonToken.VALUE_STRING) {
      return findSerializer(OType.STRING);
    }

    throw new ODatabaseException("Invalid type of JSON token encountered " + token);
  }

  public String fieldTypeId(final OType type) {
    return TYPE_SERIALIZERS.get(type).typeId();
  }

  public OType typeById(final String typeId) {
    final OType type = ID_TYPE_MAP.get(typeId);
    if (type == null) {
      throw new IllegalArgumentException("Invalid type id");
    }

    return type;
  }
}

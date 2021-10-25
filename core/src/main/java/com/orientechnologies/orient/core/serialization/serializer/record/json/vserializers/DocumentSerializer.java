package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.InvalidRecordTypeException;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class DocumentSerializer implements ValueSerializer {
  public static final String ATTRIBUTE_FIELD_TYPES = "@fieldTypes";
  public static final String ATTRIBUTE_RECORD_TYPE = "@type";
  public static final String ATTRIBUTE_CLASS = "@class";
  public static final String ATTRIBUTE_RID = "@rid";
  public static final String ATTRIBUTE_VERSION = "@version";

  public static final DocumentSerializer INSTANCE = new DocumentSerializer();

  @Override
  public void toJSON(final JsonGenerator generator, final Object record) throws IOException {
    if (record instanceof ODocument) {
      toJSON(generator, (ODocument) record);
      return;
    }

    throw new ODatabaseException("Invalid type of record " + record.getClass());
  }

  @Override
  public Object fromJSON(JsonParser parser, ODocument owner) throws IOException {
    final ODocument document = new ODocument();

    if (owner == null) {
      final JsonToken firstToken = parser.nextToken();
      if (firstToken != JsonToken.START_OBJECT) {
        throw new ODatabaseException(
            "Invalid token " + firstToken + ". Start of the object expected.");
      }
    }

    Map<String, String> fieldTypeIDs = null;
    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;

    JsonToken token = parser.nextToken();
    while (JsonToken.END_OBJECT != token) {
      final String fieldName = parser.getValueAsString();

      if (isSpecialField(fieldName)) {
        final Optional<Map<String, String>> extractedFieldTypes =
            processSpecialField(parser, document, fieldName);
        if (extractedFieldTypes.isPresent()) {
          fieldTypeIDs = extractedFieldTypes.get();
        }
      } else {
        if (fieldTypeIDs == null) {
          throw new ODatabaseException(
              "Invalid JSON format, types of the fields should be defined first");
        }

        final String fieldTypeId = fieldTypeIDs.get(fieldName);
        if (fieldTypeId == null) {
          throw new ODatabaseException("Type of the field " + fieldName + " was not provided");
        }

        final JsonToken fieldValueToken = parser.nextToken();
        final OType fieldType = serializerFactory.typeById(fieldTypeId);

        if (fieldValueToken == JsonToken.VALUE_NULL) {
          document.field(fieldName, null, fieldType);
        } else {
          final ValueSerializer valueSerializer =
              serializerFactory.findSerializer(fieldType, fieldValueToken);
          final Object value = valueSerializer.fromJSON(parser, owner);
          document.field(fieldName, value, fieldType);
        }
      }

      token = parser.nextToken();
    }

    return document;
  }

  private Optional<Map<String, String>> processSpecialField(
      JsonParser parser, ODocument document, String fieldName) throws IOException {
    final JsonToken specialTokenValue = parser.nextToken();

    if (JsonToken.VALUE_STRING != specialTokenValue) {
      throw new ODatabaseException(
          "Invalid token "
              + specialTokenValue
              + " for the special field with name "
              + fieldName
              + " string was expected");
    }

    final String specialValue = parser.getValueAsString();

    return handleMetadataField(document, fieldName, specialValue);
  }

  private Optional<Map<String, String>> handleMetadataField(
      final ODocument document, final String fieldName, final String specialValue) {
    switch (fieldName) {
      case ATTRIBUTE_CLASS:
        {
          document.setClassName(specialValue);
          return Optional.empty();
        }
      case ATTRIBUTE_RID:
        {
          final ORecordId rid = new ORecordId(specialValue);
          ORecordInternal.setIdentity(document, rid);
          return Optional.empty();
        }
      case ATTRIBUTE_VERSION:
        {
          final int version = Integer.parseInt(specialValue);
          ORecordInternal.setVersion(document, version);
          return Optional.empty();
        }
      case ATTRIBUTE_RECORD_TYPE:
        {
          if (specialValue.length() != 1 && specialValue.charAt(0) != ODocument.RECORD_TYPE) {
            throw new InvalidRecordTypeException();
          }
          return Optional.empty();
        }

      case ATTRIBUTE_FIELD_TYPES:
        {
          final Map<String, String> fieldTypes = extractFieldTypes(specialValue);
          return Optional.of(fieldTypes);
        }

      default:
        throw new ODatabaseException("Invalid metadata field " + fieldName);
    }
  }

  private static boolean isSpecialField(final String fieldName) {
    return fieldName != null && fieldName.length() > 0 && fieldName.charAt(0) == '@';
  }

  @Override
  public String typeId() {
    return SerializerIDs.EMBEDDED;
  }

  @Override
  public JsonToken[] startTokens() {
    return new JsonToken[] {JsonToken.START_OBJECT};
  }

  private void toJSON(final JsonGenerator generator, final ODocument document) throws IOException {
    final SerializerFactory serializerFactory = SerializerFactory.INSTANCE;
    generator.writeStartObject();
    try {
      writeMetadata(generator, document, serializerFactory);

      if (!document.isEmbedded()) {
        generator.writeStringField(ATTRIBUTE_RID, document.getIdentity().toString());
      }

      for (final String fieldName : document.fieldNames()) {
        final Object filedValue = document.getProperty(fieldName);
        if (filedValue == null) {
          generator.writeNullField(fieldName);
        } else {
          final OType type = document.fieldType(fieldName);
          assert type != null;

          final ValueSerializer serializer = serializerFactory.findSerializer(type);
          generator.writeFieldName(fieldName);
          serializer.toJSON(generator, filedValue);
        }
      }
    } finally {
      generator.writeEndObject();
    }
  }

  private void writeMetadata(
      final JsonGenerator generator,
      final ODocument document,
      final SerializerFactory serializerFactory)
      throws IOException {
    generator.writeStringField(ATTRIBUTE_VERSION, Integer.toString(document.getVersion()));
    generator.writeStringField(
        ATTRIBUTE_RECORD_TYPE, Character.toString((char) ORecordInternal.getRecordType(document)));

    final OClass cls = document.getSchemaClass();
    if (cls != null) {
      generator.writeStringField(ATTRIBUTE_CLASS, cls.getName());
    }

    writeFieldTypes(generator, document, serializerFactory);
  }

  private void writeFieldTypes(
      JsonGenerator generator, ODocument document, SerializerFactory serializerFactory)
      throws IOException {
    final StringBuilder fieldTypes = new StringBuilder();

    for (final String fieldName : document.fieldNames()) {
      final OType type = document.fieldType(fieldName);
      if (type == null) {
        continue;
      }

      final String typeId = serializerFactory.fieldTypeId(type);

      fieldTypes.append(fieldName).append("=").append(typeId).append(",");
    }

    if (fieldTypes.length() > 0) {
      fieldTypes.deleteCharAt(fieldTypes.length() - 1);
    }

    generator.writeStringField(ATTRIBUTE_FIELD_TYPES, fieldTypes.toString());
  }

  private HashMap<String, String> extractFieldTypes(final String fieldTypesStr) {
    if (fieldTypesStr.length() == 0) {
      return new HashMap<>();
    }

    final String[] fieldTypesMerged = fieldTypesStr.split(",");

    final HashMap<String, String> fieldTypes = new HashMap<>();
    for (final String filedTypeMerged : fieldTypesMerged) {
      final int typeSeparatorIndex = filedTypeMerged.indexOf('=');
      if (typeSeparatorIndex < 0) {
        throw new ODatabaseException("Invalid value of field types metadata : " + fieldTypesStr);
      }

      fieldTypes.put(
          filedTypeMerged.substring(0, typeSeparatorIndex),
          filedTypeMerged.substring(typeSeparatorIndex + 1));
    }

    return fieldTypes;
  }
}

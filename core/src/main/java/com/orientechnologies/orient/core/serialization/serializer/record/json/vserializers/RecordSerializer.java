package com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.json.SerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.json.ValueSerializer;

import java.io.IOException;

public final class RecordSerializer implements ValueSerializer {
  public static final String ATTRIBUTE_FIELD_TYPES = "@fieldTypes";
  public static final String ATTRIBUTE_RECORD_TYPE = "@type";
  public static final String ATTRIBUTE_CLASS = "@class";
  public static final String ATTRIBUTE_RID = "@rid";

  public static final RecordSerializer INSTANCE = new RecordSerializer();

  @Override
  public void toJSON(final JsonGenerator generator, final Object record) throws IOException {
    if (record instanceof ODocument) {
      toJSON(generator, (ODocument) record);
      return;
    }

    throw new ODatabaseException("Invalid type of record " + record.getClass());
  }

  @Override
  public String typeId() {
    return SerializerIDs.EMBEDDED;
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
    generator.writeStringField(ATTRIBUTE_RECORD_TYPE, "" + ORecordInternal.getRecordType(document));

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
    fieldTypes.deleteCharAt(fieldTypes.length() - 1);

    generator.writeStringField(ATTRIBUTE_FIELD_TYPES, fieldTypes.toString());
  }
}

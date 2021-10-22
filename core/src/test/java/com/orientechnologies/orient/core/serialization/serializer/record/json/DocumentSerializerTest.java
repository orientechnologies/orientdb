package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers.DocumentSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;

public class DocumentSerializerTest {
  @Test
  public void serializeEmptyDocument() throws Exception {
    final OrientDBConfig config =
        OrientDBConfig.builder().addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, true).build();

    try (final OrientDB orientDB =
        new OrientDB("memory:target/documentSerializerTest", "admin", "admin", config)) {
      orientDB.create("test", ODatabaseType.MEMORY);
      try (final ODatabaseSession session = orientDB.open("test", "admin", "admin")) {
        final JsonFactory factory = new JsonFactory();
        final StringWriter writer = new StringWriter();

        final JsonGenerator generator = factory.createGenerator(writer);

        final ODocument document = new ODocument("TestClass");
        ORecordInternal.setVersion(document, 42);
        ORecordInternal.setIdentity(document, new ORecordId(24, 34));

        final DocumentSerializer serializer = DocumentSerializer.INSTANCE;

        serializer.toJSON(generator, document);
        generator.close();

        final String json = writer.toString();

        final JsonParser parser = factory.createParser(json);

        final ODocument resultedDocument = (ODocument) serializer.fromJSON(parser, null);

        final ODatabaseDocumentInternal sessionInternal  = (ODatabaseDocumentInternal) session;

        Assert.assertEquals(document.getClassName(), resultedDocument.getClassName());
        Assert.assertEquals(document.getVersion(), resultedDocument.getVersion());
        Assert.assertEquals(document.getIdentity(), resultedDocument.getIdentity());

        Assert.assertTrue(ODocumentHelper.hasSameContentOf(document, sessionInternal, resultedDocument,
                sessionInternal, null));
      }
    }
  }
}

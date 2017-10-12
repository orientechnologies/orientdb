package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.After;

import java.io.File;
import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 19.02.13
 */
public class OLocalHashTableTestIT extends OLocalHashTableBase {

  public OLocalHashTableTestIT() throws Exception {

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    final File dbDirectory = new File(buildDirectory, "localHashTableTest");
    System.out.println(this.getClass().getSimpleName() + " test is initializing using DB directory = " + dbDirectory);

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + dbDirectory.getCanonicalPath());
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>();
    murmurHash3HashFunction.setValueSerializer(OIntegerSerializer.INSTANCE);

    localHashTable = new OLocalHashTable<Integer, String>("localHashTableTest", ".imc", ".tsc", ".obf", ".nbh",
        murmurHash3HashFunction, (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());

    localHashTable
        .create(OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance().<String>getObjectSerializer(OType.STRING), null,
            true);

  }

  @After
  public void afterMethod() throws IOException {
    localHashTable.clear();
  }

}

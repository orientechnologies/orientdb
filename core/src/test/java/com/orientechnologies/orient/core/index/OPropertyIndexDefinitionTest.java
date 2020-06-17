package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OPropertyIndexDefinitionTest {
  private OPropertyIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new OPropertyIndexDefinition("testClass", "fOne", OType.INTEGER);
  }

  @Test
  public void testCreateValueSingleParameter() {
    final Object result = propertyIndex.createValue(Collections.singletonList("12"));
    Assert.assertEquals(result, 12);
  }

  @Test
  public void testCreateValueTwoParameters() {
    final Object result = propertyIndex.createValue(Arrays.asList("12", "25"));
    Assert.assertEquals(result, 12);
  }

  @Test(expected = NumberFormatException.class)
  public void testCreateValueWrongParameter() {
    propertyIndex.createValue(Collections.singletonList("tt"));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    final Object result = propertyIndex.createValue("12");
    Assert.assertEquals(result, 12);
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    final Object result = propertyIndex.createValue("12", "25");
    Assert.assertEquals(result, 12);
  }

  @Test(expected = NumberFormatException.class)
  public void testCreateValueWrongParameterArrayParams() {
    propertyIndex.createValue("tt");
  }

  @Test
  public void testGetDocumentValueToIndex() {
    final ODocument document = new ODocument();

    document.field("fOne", "15");
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(document);
    Assert.assertEquals(result, 15);
  }

  @Test
  public void testGetFields() {
    final List<String> result = propertyIndex.getFields();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), "fOne");
  }

  @Test
  public void testGetTypes() {
    final OType[] result = propertyIndex.getTypes();
    Assert.assertEquals(result.length, 1);
    Assert.assertEquals(result[0], OType.INTEGER);
  }

  @Test
  public void testEmptyIndexReload() {
    final ODatabaseDocumentTx database = new ODatabaseDocumentTx("memory:propertytest");
    database.create();

    propertyIndex = new OPropertyIndexDefinition("tesClass", "fOne", OType.INTEGER);

    final ODocument docToStore = propertyIndex.toStream();
    database.save(docToStore, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docToLoad = database.load(docToStore.getIdentity());

    final OPropertyIndexDefinition result = new OPropertyIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
    Assert.assertEquals(result, propertyIndex);
  }

  @Test
  public void testIndexReload() {
    final ODocument docToStore = propertyIndex.toStream();

    final OPropertyIndexDefinition result = new OPropertyIndexDefinition();
    result.fromStream(docToStore);

    Assert.assertEquals(result, propertyIndex);
  }

  @Test
  public void testGetParamCount() {
    Assert.assertEquals(propertyIndex.getParamCount(), 1);
  }

  @Test
  public void testClassName() {
    Assert.assertEquals("testClass", propertyIndex.getClassName());
  }
}

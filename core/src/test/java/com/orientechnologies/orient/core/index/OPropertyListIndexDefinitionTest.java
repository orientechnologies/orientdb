package com.orientechnologies.orient.core.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 20.12.11
 */
@Test
public class OPropertyListIndexDefinitionTest
{
  private OPropertyListIndexDefinition propertyIndex;

  @BeforeMethod
  public void beforeMethod() {
    propertyIndex = new OPropertyListIndexDefinition("testClass", "fOne", OType.INTEGER);
  }

  @Test
  public void testCreateValueSingleParameter() {
    final Object result = propertyIndex.createValue( Collections.singletonList( Arrays.asList( "12", "23" ) ) );

    Assert.assertTrue( result instanceof Collection );

    final Collection collectionResult = (Collection) result;
    Assert.assertEquals( collectionResult.size(), 2 );

    Assert.assertTrue( collectionResult.contains( 12 ) );
    Assert.assertTrue( collectionResult.contains( 23 ) );
  }

  @Test
  public void testCreateValueTwoParameters() {
    final Object result = propertyIndex.createValue( Arrays.asList( Arrays.asList( "12", "23" ), "25" ) );

    Assert.assertTrue( result instanceof Collection );

    final Collection collectionResult = (Collection) result;
    Assert.assertEquals( collectionResult.size(), 2 );

    Assert.assertTrue( collectionResult.contains( 12 ) );
    Assert.assertTrue( collectionResult.contains( 23 ) );
  }

  @Test
  public void testCreateValueWrongParameter() {
    final Object result = propertyIndex.createValue( Collections.singletonList( "tt" ) );
    Assert.assertNull(result);
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    final Object result = propertyIndex.createValue( (Object) Arrays.asList( "12", "23" ) );

    Assert.assertTrue( result instanceof Collection );

    final Collection collectionResult = (Collection) result;
    Assert.assertEquals( collectionResult.size(), 2 );

    Assert.assertTrue( collectionResult.contains( 12 ) );
    Assert.assertTrue( collectionResult.contains( 23 ) );
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    final Object result = propertyIndex.createValue( Arrays.asList( "12", "23" ), "25" );

    Assert.assertTrue( result instanceof Collection );

    final Collection collectionResult = (Collection) result;
    Assert.assertEquals( collectionResult.size(), 2 );

    Assert.assertTrue( collectionResult.contains( 12 ) );
    Assert.assertTrue( collectionResult.contains( 23 ) );
  }

  @Test
  public void testCreateValueWrongParameterArrayParams() {
    final Object result = propertyIndex.createValue( "tt" );
    Assert.assertNull(result);
  }

  @Test
  public void testGetDocumentValueToIndex() {
    final ODocument document = new ODocument();

    document.field("fOne", Arrays.asList( "12", "23" ));
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(document);
    Assert.assertTrue( result instanceof Collection );

    final Collection collectionResult = (Collection) result;
    Assert.assertEquals( collectionResult.size(), 2 );

    Assert.assertTrue( collectionResult.contains( 12 ) );
    Assert.assertTrue( collectionResult.contains( 23 ) );
  }

  @Test
  public void testCreateSingleValue() {
    final Object result = propertyIndex.createSingleValue( "12" );
    Assert.assertEquals(result, 12);
  }

  @Test
  public void testCreateSingleValueWrongParameter() {
    final Object result = propertyIndex.createSingleValue( "tt" );
    Assert.assertNull(result);
  }

}

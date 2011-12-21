package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

@Test(groups = {"index"})
public class ClassIndexTest
{
  private final ODatabaseDocumentTx database;
  private OClass oClass;
  private OClass oSuperClass;

  @Parameters(value = "url")
  public ClassIndexTest( final String iURL )
  {
    database = new ODatabaseDocumentTx( iURL );
  }

  @BeforeClass
  public void beforeClass()
  {
    if ( database.isClosed() ) {
      database.open( "admin", "admin" );
    }

    final OSchema schema = database.getMetadata().getSchema();

    oClass = schema.createClass( "ClassIndexTestClass" );
    oSuperClass = schema.createClass( "ClassIndexTestSuperClass" );


    oClass.createProperty( "fOne", OType.INTEGER );
    oClass.createProperty( "fTwo", OType.STRING );
    oClass.createProperty( "fThree", OType.BOOLEAN );
    oClass.createProperty( "fFour", OType.INTEGER );

    oClass.createProperty( "fSix", OType.STRING );
    oClass.createProperty( "fSeven", OType.STRING );
    oClass.createProperty( "fEmbeddedMap", OType.EMBEDDEDMAP, OType.INTEGER );
    oClass.createProperty( "fLinkMap", OType.LINKMAP );

    oSuperClass.createProperty( "fNine", OType.INTEGER );
    oClass.setSuperClass( oSuperClass );

    schema.save();
    database.close();
  }

  @BeforeMethod
  public void beforeMethod()
  {
    database.open( "admin", "admin" );
  }

  @AfterMethod
  public void afterMethod()
  {
    database.close();
  }

  @AfterClass
  public void afterClass()
  {
    if ( database.isClosed() ) {
      database.open( "admin", "admin" );
    }

    database.command( new OCommandSQL( "delete from ClassIndexTestClass" ) ).execute();
    database.command( new OCommandSQL( "delete from ClassIndexTestSuperClass" ) ).execute();
    database.command( new OCommandSQL( "delete from ClassIndexInTest" ) ).execute();

    database.command( new OCommandSQL( "drop class ClassIndexInTest" ) ).execute();
    database.command( new OCommandSQL( "drop class ClassIndexTestClass" ) ).execute();

    database.getMetadata().getSchema().reload();

    database.command( new OCommandSQL( "drop class ClassIndexTestSuperClass" ) ).execute();

    database.getMetadata().getSchema().reload();
    database.getMetadata().getIndexManager().reload();

    database.close();
  }

  @Test
  public void testCreateOnePropertyIndexTest()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne" );

    assertEquals( result.getName(), "ClassIndexTestPropertyOne" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyOne" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyOne" ).getName(),
      result.getName() );

  }

  @Test
  public void createCompositeIndexTestWithoutListener()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestCompositeOne", OClass.INDEX_TYPE.UNIQUE, "fOne", "fTwo" );

    assertEquals( result.getName(), "ClassIndexTestCompositeOne" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestCompositeOne" ).getName(), result.getName() );
    assertEquals( database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestCompositeOne" ).getName(),
      result.getName() );
  }

  @Test
  public void createCompositeIndexTestWithListener()
  {
    final AtomicInteger atomicInteger = new AtomicInteger( 0 );
    final OProgressListener progressListener = new OProgressListener()
    {
      public void onBegin( final Object iTask, final long iTotal )
      {
        atomicInteger.incrementAndGet();
      }

      public boolean onProgress( final Object iTask, final long iCounter, final float iPercent )
      {
        return true;
      }

      public void onCompletition( final Object iTask, final boolean iSucceed )
      {
        atomicInteger.incrementAndGet();
      }
    };

    final OIndex result = oClass.createIndex( "ClassIndexTestCompositeTwo", OClass.INDEX_TYPE.UNIQUE,
      progressListener, "fOne", "fTwo", "fThree" );

    assertEquals( result.getName(), "ClassIndexTestCompositeTwo" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestCompositeTwo" ).getName(), result.getName() );
    assertEquals( database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestCompositeTwo" ).getName(),
      result.getName() );
    assertEquals( atomicInteger.get(), 2 );
  }

  @Test
  public void testCreateOnePropertyEmbeddedMapIndex()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap" );

    assertEquals( result.getName(), "ClassIndexTestPropertyEmbeddedMap" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyEmbeddedMap" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyEmbeddedMap" ).getName(),
      result.getName() );

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue( indexDefinition instanceof OPropertyMapIndexDefinition );
    assertEquals( indexDefinition.getFields().get( 0 ), "fEmbeddedMap" );
    assertEquals( indexDefinition.getTypes()[0], OType.STRING );
    assertEquals(((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateOnePropertyLinkedMapIndex()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyLinkedMap", OClass.INDEX_TYPE.UNIQUE, "fLinkMap" );

    assertEquals( result.getName(), "ClassIndexTestPropertyLinkedMap" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyLinkedMap" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyLinkedMap" ).getName(),
      result.getName() );

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue( indexDefinition instanceof OPropertyMapIndexDefinition );
    assertEquals( indexDefinition.getFields().get( 0 ), "fLinkMap" );
    assertEquals( indexDefinition.getTypes()[0], OType.STRING );
    assertEquals(((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateOnePropertyLinkMapByKeyIndex()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyLinkedMap", OClass.INDEX_TYPE.UNIQUE, "fLinkMap by key" );

    assertEquals( result.getName(), "ClassIndexTestPropertyLinkedMap" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyLinkedMap" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyLinkedMap" ).getName(),
      result.getName() );

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue( indexDefinition instanceof OPropertyMapIndexDefinition );
    assertEquals( indexDefinition.getFields().get( 0 ), "fLinkMap" );
    assertEquals( indexDefinition.getTypes()[0], OType.STRING );
    assertEquals(((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateOnePropertyLinkMapByValueIndex()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyLinkedMap", OClass.INDEX_TYPE.UNIQUE, "fLinkMap by value" );

    assertEquals( result.getName(), "ClassIndexTestPropertyLinkedMap" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyLinkedMap" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyLinkedMap" ).getName(),
      result.getName() );

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue( indexDefinition instanceof OPropertyMapIndexDefinition );
    assertEquals( indexDefinition.getFields().get( 0 ), "fLinkMap" );
    assertEquals( indexDefinition.getTypes()[0], OType.LINK );
    assertEquals(((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.VALUE );
  }


  @Test
  public void testCreateOnePropertyByKeyEmbeddedMapIndex()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyByKeyEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap by key" );

    assertEquals( result.getName(), "ClassIndexTestPropertyByKeyEmbeddedMap" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyByKeyEmbeddedMap" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyByKeyEmbeddedMap" ).getName(),
      result.getName() );

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue( indexDefinition instanceof OPropertyMapIndexDefinition );
    assertEquals( indexDefinition.getFields().get( 0 ), "fEmbeddedMap" );
    assertEquals( indexDefinition.getTypes()[0], OType.STRING );
    assertEquals(((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.KEY );
  }

  @Test
  public void testCreateOnePropertyByValueEmbeddedMapIndex()
  {
    final OIndex result = oClass.createIndex( "ClassIndexTestPropertyByValueEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap by value" );

    assertEquals( result.getName(), "ClassIndexTestPropertyByValueEmbeddedMap" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestPropertyByValueEmbeddedMap" ).getName(), result.getName() );
    assertEquals(
      database.getMetadata().getIndexManager().getClassIndex( "ClassIndexTestClass", "ClassIndexTestPropertyByValueEmbeddedMap" ).getName(),
      result.getName() );

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue( indexDefinition instanceof OPropertyMapIndexDefinition );
    assertEquals( indexDefinition.getFields().get( 0 ), "fEmbeddedMap" );
    assertEquals( indexDefinition.getTypes()[0], OType.INTEGER );
    assertEquals(((OPropertyMapIndexDefinition)indexDefinition).getIndexBy(), OPropertyMapIndexDefinition.INDEX_BY.VALUE );
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexOne()
  {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex( "ClassIndexTestPropertyWrongSpecifierEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap by ttt" );
    } catch( IllegalArgumentException e ) {
      exceptionIsThrown = true;
      assertEquals(e.getMessage(), "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap by ttt'" );
    }

    assertTrue( exceptionIsThrown );
    assertNull( oClass.getClassIndex( "ClassIndexTestPropertyWrongSpecifierEmbeddedMap" ));
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexTwo()
  {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex( "ClassIndexTestPropertyWrongSpecifierEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap b value" );
    } catch( IllegalArgumentException e ) {
      exceptionIsThrown = true;
      assertEquals(e.getMessage(), "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap b value'" );
    }

    assertTrue( exceptionIsThrown );
    assertNull( oClass.getClassIndex( "ClassIndexTestPropertyWrongSpecifierEmbeddedMap" ));
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexThree()
  {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex( "ClassIndexTestPropertyWrongSpecifierEmbeddedMap", OClass.INDEX_TYPE.UNIQUE, "fEmbeddedMap by value t" );
    } catch( IllegalArgumentException e ) {
      exceptionIsThrown = true;
      assertEquals(e.getMessage(), "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap by value t'" );
    }

    assertTrue( exceptionIsThrown );
    assertNull( oClass.getClassIndex( "ClassIndexTestPropertyWrongSpecifierEmbeddedMap" ));
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedOneProperty()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fOne" ) );

    assertTrue( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedDoesNotContainProperty()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fSix" ) );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedTwoProperties()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fTwo", "fOne" ) );

    assertTrue( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedThreeProperties()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fTwo", "fOne", "fThree" ) );

    assertTrue( result );
  }


  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedPropertiesNotFirst()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fTwo", "fTree" ) );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedPropertiesMoreThanNeeded()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fTwo", "fOne", "fThee", "fFour" ) );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "createParentPropertyIndex", "testCreateOnePropertyEmbeddedMapIndex",
    "testCreateOnePropertyByKeyEmbeddedMapIndex", "testCreateOnePropertyByValueEmbeddedMapIndex",
    "testCreateOnePropertyLinkedMapIndex", "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedParentProperty()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fNine" ) );

    assertTrue( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex"
    , "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedParentChildProperty()
  {
    final boolean result = oClass.areIndexed( Arrays.asList( "fOne, fNine" ) );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedOnePropertyArrayParams()
  {
    final boolean result = oClass.areIndexed( "fOne" );

    assertTrue( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedDoesNotContainPropertyArrayParams()
  {
    final boolean result = oClass.areIndexed( "fSix" );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedTwoPropertiesArrayParams()
  {
    final boolean result = oClass.areIndexed( "fTwo", "fOne" );

    assertTrue( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedThreePropertiesArrayParams()
  {
    final boolean result = oClass.areIndexed( "fTwo", "fOne", "fThree" );

    assertTrue( result );
  }


  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedPropertiesNotFirstArrayParams()
  {
    final boolean result = oClass.areIndexed( "fTwo", "fTree" );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams()
  {
    final boolean result = oClass.areIndexed( "fTwo", "fOne", "fThee", "fFour" );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "createParentPropertyIndex", "testCreateOnePropertyEmbeddedMapIndex",
    "testCreateOnePropertyByKeyEmbeddedMapIndex", "testCreateOnePropertyByValueEmbeddedMapIndex",
    "testCreateOnePropertyLinkedMapIndex", "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedParentPropertyArrayParams()
  {
    final boolean result = oClass.areIndexed( "fNine" );

    assertTrue( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testAreIndexedParentChildPropertyArrayParams()
  {
    final boolean result = oClass.areIndexed( "fOne, fNine" );

    assertFalse( result );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( "fOne" );

    assertEquals( result.size(), 3 );

    assertTrue( containsIndex( result, "ClassIndexTestPropertyOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesTwoPropertiesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( "fTwo", "fOne" );
    assertEquals( result.size(), 2 );

    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesThreePropertiesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( "fTwo", "fOne", "fThree" );

    assertEquals( result.size(), 1 );
    assertEquals( result.iterator().next().getName(), "ClassIndexTestCompositeTwo" );
  }


  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( "fTwo", "fFour" );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( "fTwo", "fOne", "fThee", "fFour" );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( Arrays.asList( "fTwo", "fOne", "fThee", "fFour" ) );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesOneProperty()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( Arrays.asList( "fOne" ) );

    assertEquals( result.size(), 3 );

    assertTrue( containsIndex( result, "ClassIndexTestPropertyOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesTwoProperties()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( Arrays.asList( "fTwo", "fOne" ) );
    assertEquals( result.size(), 2 );

    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesThreeProperties()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( Arrays.asList( "fTwo", "fOne", "fThree" ) );

    assertEquals( result.size(), 1 );
    assertEquals( result.iterator().next().getName(), "ClassIndexTestCompositeTwo" );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesNotInvolvedProperties()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( Arrays.asList( "fTwo", "fFour" ) );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded()
  {
    final Set<OIndex<?>> result = oClass.getClassInvolvedIndexes( Arrays.asList( "fTwo", "fOne", "fThee", "fFour" ) );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesOnePropertyArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( "fOne" );

    assertEquals( result.size(), 3 );

    assertTrue( containsIndex( result, "ClassIndexTestPropertyOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesTwoPropertiesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( "fTwo", "fOne" );
    assertEquals( result.size(), 2 );

    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesThreePropertiesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( "fTwo", "fOne", "fThree" );

    assertEquals( result.size(), 1 );
    assertEquals( result.iterator().next().getName(), "ClassIndexTestCompositeTwo" );
  }


  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesNotInvolvedPropertiesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( "fTwo", "fFour" );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetParentInvolvedIndexesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( "fNine" );

    assertEquals( result.size(), 1 );
    assertEquals( result.iterator().next().getName(), "ClassIndexTestParentPropertyNine" );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetParentChildInvolvedIndexesArrayParams()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( "fOne", "fNine" );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesOneProperty()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( Arrays.asList( "fOne" ) );

    assertEquals( result.size(), 3 );

    assertTrue( containsIndex( result, "ClassIndexTestPropertyOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesTwoProperties()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( Arrays.asList( "fTwo", "fOne" ) );
    assertEquals( result.size(), 2 );

    assertTrue( containsIndex( result, "ClassIndexTestCompositeOne" ) );
    assertTrue( containsIndex( result, "ClassIndexTestCompositeTwo" ) );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesThreeProperties()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( Arrays.asList( "fTwo", "fOne", "fThree" ) );

    assertEquals( result.size(), 1 );
    assertEquals( result.iterator().next().getName(), "ClassIndexTestCompositeTwo" );
  }


  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetInvolvedIndexesNotInvolvedProperties()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( Arrays.asList( "fTwo", "fFour" ) );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetParentInvolvedIndexes()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( Arrays.asList( "fNine" ) );

    assertEquals( result.size(), 1 );
    assertEquals( result.iterator().next().getName(), "ClassIndexTestParentPropertyNine" );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetParentChildInvolvedIndexes()
  {
    final Set<OIndex<?>> result = oClass.getInvolvedIndexes( Arrays.asList( "fOne", "fNine" ) );

    assertEquals( result.size(), 0 );
  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "testCreateOnePropertyEmbeddedMapIndex", "testCreateOnePropertyByKeyEmbeddedMapIndex",
    "testCreateOnePropertyByValueEmbeddedMapIndex", "testCreateOnePropertyLinkedMapIndex",
    "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetClassIndexes()
  {
    final Set<OIndex<?>> indexes = oClass.getClassIndexes();
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

    final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition( "ClassIndexTestClass" );

    compositeIndexOne.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fOne", OType.INTEGER ) );
    compositeIndexOne.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fTwo", OType.STRING ) );
    expectedIndexDefinitions.add( compositeIndexOne );

    final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition( "ClassIndexTestClass" );

    compositeIndexTwo.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fOne", OType.INTEGER ) );
    compositeIndexTwo.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fTwo", OType.STRING ) );
    compositeIndexTwo.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fThree", OType.BOOLEAN ) );
    expectedIndexDefinitions.add( compositeIndexTwo );

    final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition( "ClassIndexTestClass", "fOne", OType.INTEGER );
    expectedIndexDefinitions.add( propertyIndex );

    final OPropertyMapIndexDefinition propertyMapIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fEmbeddedMap", OType.STRING,
      OPropertyMapIndexDefinition.INDEX_BY.KEY );
    expectedIndexDefinitions.add( propertyMapIndexDefinition );

    final OPropertyMapIndexDefinition propertyMapByValueIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fEmbeddedMap", OType.INTEGER,
      OPropertyMapIndexDefinition.INDEX_BY.VALUE );
    expectedIndexDefinitions.add( propertyMapByValueIndexDefinition );

    final OPropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fLinkMap", OType.STRING,
      OPropertyMapIndexDefinition.INDEX_BY.KEY );
    expectedIndexDefinitions.add( propertyLinkMapByKeyIndexDefinition );

    final OPropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fLinkMap", OType.LINK,
      OPropertyMapIndexDefinition.INDEX_BY.VALUE );
    expectedIndexDefinitions.add( propertyLinkMapByValueIndexDefinition );

    assertEquals( indexes.size(), 7);

    for( final OIndex index : indexes ) {
      assertTrue( expectedIndexDefinitions.contains( index.getDefinition() ) );
    }

  }

  @Test(dependsOnMethods = {
    "createCompositeIndexTestWithListener", "createCompositeIndexTestWithoutListener",
    "testCreateOnePropertyIndexTest", "createParentPropertyIndex", "testCreateOnePropertyEmbeddedMapIndex",
    "testCreateOnePropertyByKeyEmbeddedMapIndex", "testCreateOnePropertyByValueEmbeddedMapIndex",
    "testCreateOnePropertyLinkedMapIndex", "testCreateOnePropertyLinkMapByKeyIndex", "testCreateOnePropertyLinkMapByValueIndex"
  })
  public void testGetIndexes()
  {
    final Set<OIndex<?>> indexes = oClass.getIndexes();
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<OIndexDefinition>();

    final OCompositeIndexDefinition compositeIndexOne = new OCompositeIndexDefinition( "ClassIndexTestClass" );

    compositeIndexOne.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fOne", OType.INTEGER ) );
    compositeIndexOne.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fTwo", OType.STRING ) );
    expectedIndexDefinitions.add( compositeIndexOne );

    final OCompositeIndexDefinition compositeIndexTwo = new OCompositeIndexDefinition( "ClassIndexTestClass" );

    compositeIndexTwo.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fOne", OType.INTEGER ) );
    compositeIndexTwo.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fTwo", OType.STRING ) );
    compositeIndexTwo.addIndex( new OPropertyIndexDefinition( "ClassIndexTestClass", "fThree", OType.BOOLEAN ) );
    expectedIndexDefinitions.add( compositeIndexTwo );

    final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition( "ClassIndexTestClass", "fOne", OType.INTEGER );
    expectedIndexDefinitions.add( propertyIndex );

    final OPropertyIndexDefinition parentPropertyIndex = new OPropertyIndexDefinition( "ClassIndexTestSuperClass", "fNine", OType.INTEGER );
    expectedIndexDefinitions.add( parentPropertyIndex );

    final OPropertyMapIndexDefinition propertyMapIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fEmbeddedMap", OType.STRING,
      OPropertyMapIndexDefinition.INDEX_BY.KEY );
    expectedIndexDefinitions.add( propertyMapIndexDefinition );

    final OPropertyMapIndexDefinition propertyMapByValueIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fEmbeddedMap", OType.INTEGER,
      OPropertyMapIndexDefinition.INDEX_BY.VALUE );
    expectedIndexDefinitions.add( propertyMapByValueIndexDefinition );

    final OPropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fLinkMap", OType.STRING,
      OPropertyMapIndexDefinition.INDEX_BY.KEY );
    expectedIndexDefinitions.add( propertyLinkMapByKeyIndexDefinition );

    final OPropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition = new OPropertyMapIndexDefinition( "ClassIndexTestClass", "fLinkMap", OType.LINK,
      OPropertyMapIndexDefinition.INDEX_BY.VALUE );
    expectedIndexDefinitions.add( propertyLinkMapByValueIndexDefinition );

    assertEquals( indexes.size(), 8 );

    for( final OIndex index : indexes ) {
      assertTrue( expectedIndexDefinitions.contains( index.getDefinition() ) );
    }
  }

  @Test
  public void testGetIndexesWithoutParent()
  {

    final OClass inClass = database.getMetadata().getSchema().createClass( "ClassIndexInTest" );
    inClass.createProperty( "fOne", OType.INTEGER );

    final OIndex result = inClass.createIndex( "ClassIndexTestPropertyOne", OClass.INDEX_TYPE.UNIQUE, "fOne" );

    assertEquals( result.getName(), "ClassIndexTestPropertyOne" );
    assertEquals( inClass.getClassIndex( "ClassIndexTestPropertyOne" ).getName(), result.getName() );

    final Set<OIndex<?>> indexes = inClass.getIndexes();
    final OPropertyIndexDefinition propertyIndexDefinition = new OPropertyIndexDefinition( "ClassIndexInTest", "fOne", OType.INTEGER );

    assertEquals( indexes.size(), 1 );

    assertTrue( indexes.iterator().next().getDefinition().equals( propertyIndexDefinition ) );
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateIndexEmptyFields()
  {
    oClass.createIndex( "ClassIndexTestCompositeEmpty", OClass.INDEX_TYPE.UNIQUE );
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateIndexAbsentFields()
  {
    oClass.createIndex( "ClassIndexTestCompositeFieldAbsent", OClass.INDEX_TYPE.UNIQUE, "fFive" );
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateProxyIndex()
  {
    oClass.createIndex( "ClassIndexTestProxyIndex", OClass.INDEX_TYPE.PROXY, "fOne" );
  }

  @Test(expectedExceptions = OIndexException.class)
  public void testCreateFullTextIndexTwoProperties()
  {
    oClass.createIndex( "ClassIndexTestFulltextIndex", OClass.INDEX_TYPE.FULLTEXT, "fSix", "fSeven" );
  }

  @Test
  public void testCreateFullTextIndexOneProperty()
  {
    final OIndex<?> result = oClass.createIndex( "ClassIndexTestFulltextIndex", OClass.INDEX_TYPE.FULLTEXT, "fSix" );

    assertEquals( result.getName(), "ClassIndexTestFulltextIndex" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestFulltextIndex" ).getName(), result.getName() );
    assertEquals( result.getType(), OClass.INDEX_TYPE.FULLTEXT.toString() );
  }

  @Test
  public void testCreateDictionaryIndex()
  {
    final OIndex<?> result = oClass.createIndex( "ClassIndexTestDictionaryIndex", OClass.INDEX_TYPE.DICTIONARY, "fOne" );

    assertEquals( result.getName(), "ClassIndexTestDictionaryIndex" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestDictionaryIndex" ).getName(), result.getName() );
    assertEquals( result.getType(), OClass.INDEX_TYPE.DICTIONARY.toString() );
  }

  @Test
  public void testCreateNotUniqueIndex()
  {
    final OIndex<?> result = oClass.createIndex( "ClassIndexTestNotUniqueIndex", OClass.INDEX_TYPE.NOTUNIQUE, "fOne" );

    assertEquals( result.getName(), "ClassIndexTestNotUniqueIndex" );
    assertEquals( oClass.getClassIndex( "ClassIndexTestNotUniqueIndex" ).getName(), result.getName() );
    assertEquals( result.getType(), OClass.INDEX_TYPE.NOTUNIQUE.toString() );
  }

  public void createParentPropertyIndex()
  {
    final OIndex result = oSuperClass.createIndex( "ClassIndexTestParentPropertyNine", OClass.INDEX_TYPE.UNIQUE, "fNine" );

    assertEquals( result.getName(), "ClassIndexTestParentPropertyNine" );
    assertEquals( oSuperClass.getClassIndex( "ClassIndexTestParentPropertyNine" ).getName(), result.getName() );
  }

  private boolean containsIndex( final Collection<? extends OIndex> classIndexes, final String indexName )
  {
    for( final OIndex index : classIndexes ) {
      if ( index.getName().equals( indexName ) ) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testDropProperty() throws Exception
  {
    oClass.createProperty( "fFive", OType.INTEGER );

    oClass.dropProperty( "fFive" );

    assertNull( oClass.getProperty( "fFive" ) );
  }
}

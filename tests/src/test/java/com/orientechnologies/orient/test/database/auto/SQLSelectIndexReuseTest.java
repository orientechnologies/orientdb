package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups = {"index"})
public class SQLSelectIndexReuseTest
{
  private final ODatabaseDocumentTx database;
  private JMXConnector jmxConnector;
  private OProfilerMBean profiler;

  @Parameters(value = "url")
  public SQLSelectIndexReuseTest( final String iURL )
  {
    database = new ODatabaseDocumentTx( iURL );
  }


  @BeforeClass
  public void beforeClass() throws Exception
  {
    if ( database.isClosed() ) {
      database.open( "admin", "admin" );
    }

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass( "sqlSelectIndexReuseTestClass" );

    oClass.createProperty( "prop1", OType.INTEGER );
    oClass.createProperty( "prop2", OType.INTEGER );
    oClass.createProperty( "prop3", OType.INTEGER );
    oClass.createProperty( "prop4", OType.INTEGER );
    oClass.createProperty( "prop5", OType.INTEGER );
    oClass.createProperty( "prop6", OType.INTEGER );
    oClass.createProperty( "prop7", OType.STRING );
    oClass.createProperty( "fEmbeddedMap", OType.EMBEDDEDMAP, OType.INTEGER );
    oClass.createProperty( "fLinkMap", OType.LINKMAP );
    oClass.createProperty( "fEmbeddedList", OType.EMBEDDEDLIST, OType.INTEGER );
    oClass.createProperty( "fLinkList", OType.LINKLIST );
    


    oClass.createIndex( "indexone", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2" );
    oClass.createIndex( "indextwo", OClass.INDEX_TYPE.UNIQUE, "prop3" );
    oClass.createIndex( "indexthree", OClass.INDEX_TYPE.NOTUNIQUE, "prop1", "prop2", "prop4" );
    oClass.createIndex( "indexfour", OClass.INDEX_TYPE.NOTUNIQUE, "prop4", "prop1", "prop3" );
    oClass.createIndex( "indexfive", OClass.INDEX_TYPE.NOTUNIQUE, "prop6", "prop1", "prop3" );
    oClass.createIndex( "indexsix", OClass.INDEX_TYPE.FULLTEXT, "prop7" );
    oClass.createIndex( "sqlSelectIndexReuseTestEmbeddedMapByKey", OClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedMap" );
    oClass.createIndex( "sqlSelectIndexReuseTestEmbeddedMapByValue", OClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedMap by value" );
    oClass.createIndex( "sqlSelectIndexReuseTestLinkMapByKey", OClass.INDEX_TYPE.NOTUNIQUE, "fLinkMap" );
    oClass.createIndex( "sqlSelectIndexReuseTestLinkMapByValue", OClass.INDEX_TYPE.NOTUNIQUE, "fLinkMap by value" );
    oClass.createIndex( "sqlSelectIndexReuseTestEmbeddedList", OClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedList" );
    oClass.createIndex( "sqlSelectIndexReuseTestLinkList", OClass.INDEX_TYPE.NOTUNIQUE, "fLinkList" );
    

    schema.save();

    final String fullTextIndexStrings[] = {
      "Alice : What is the use of a book, without pictures or conversations?",
      "Rabbit : Oh my ears and whiskers, how late it's getting!",
      "Alice : If it had grown up, it would have made a dreadfully ugly child; but it makes rather a handsome pig, I think",
      "The Cat : We're all mad here.",
      "The Hatter : Why is a raven like a writing desk?",
      "The Hatter : Twinkle, twinkle, little bat! How I wonder what you're at.",
      "The Queen : Off with her head!",
      "The Duchess : Tut, tut, child! Everything's got a moral, if only you can find it.",
      "The Duchess : Take care of the sense, and the sounds will take care of themselves.",
      "The King : Begin at the beginning and go on till you come to the end: then stop."
    };


    final ORID[] links = new ORID[10];
    for(int i = 0; i < 10; i++) {
      final ODocument document = new ODocument( database, "sqlSelectIndexReuseTestClass" );
      document.field( "ftosearch", "ftosearch" );
      document.save();
      links[i] = document.getIdentity();
    }


    for( int i = 0; i < 10; i++ ) {
      final Map<String, Integer> embeddedMap = new HashMap<String, Integer>(  );
      final Map<String, ORID> linkMap = new HashMap<String, ORID>(  );
        
      embeddedMap.put( "key" + (i * 10 + 1), i*10 + 1 );
      embeddedMap.put( "key" + (i * 10 + 2), i*10 + 2 );
      embeddedMap.put( "key" + (i * 10 + 3), i*10 + 3 );
      embeddedMap.put( "key" + (i * 10 + 4), i*10 + 1 );

      linkMap.put( "key" + (i * 10 + 1), links[i] );
      linkMap.put( "key" + (i * 10 + 2), links[i] );
      linkMap.put( "key" + (i * 10 + 3), links[i] );
      linkMap.put( "key" + (i * 10 + 4), links[i] );

      final List<Integer> embeddedList = new ArrayList<Integer>( 3 );
      embeddedList.add( i * 3 );
      embeddedList.add( i * 3 + 1);
      embeddedList.add( i * 3 + 2);

      final List<ORID> linkList = new ArrayList<ORID>( 2 );
      if(i < 9) {
        linkList.add( links[i] );
        linkList.add( links[i + 1] );
      } else
        linkList.add( links[i] );
        
      
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument( database, "sqlSelectIndexReuseTestClass" );
        document.field( "prop1", i );
        document.field( "prop2", j );
        document.field( "prop3", i * 10 + j );

        document.field( "prop4", i );
        document.field( "prop5", i );

        document.field( "prop6", j );

        document.field( "prop7", fullTextIndexStrings[i] );

        document.field( "fEmbeddedMap", embeddedMap );
        document.field( "fLinkMap", linkMap );

        document.field( "fEmbeddedList", embeddedList );
        document.field( "fLinkList", new ArrayList<ORID>( linkList ) );

        document.save();
      }
    }
    profiler = getProfilerInstance();
    database.close();

    if ( !profiler.isRecording() ) {
      profiler.startRecording();
    }
  }

  @BeforeMethod
  public void beforeMethod()
  {
    if ( database.isClosed() ) {
      database.open( "admin", "admin" );
    }
  }

  @AfterMethod
  public void afterMethod()
  {
    database.close();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    if ( database.isClosed() ) {
      database.open( "admin", "admin" );
    }

    database.command( new OCommandSQL( "drop class sqlSelectIndexReuseTestClass" ) ).execute();
    database.getMetadata().getSchema().reload();
    database.getLevel2Cache().clear();

    database.close();
    closeJMXConnector();
  }

  @Test
  public void testCompositeSearchEquals()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 2" ) ).execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 2 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchHasChainOperatorsEquals()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1.asInteger() = 1 and prop2 = 2" ) ).execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 2 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchEqualsOneField()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testNoCompositeSearchEquals()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 = 1" ) ).execute();

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", i );
      document.field( "prop2", 1 );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }
  }

  @Test
  public void testCompositeSearchEqualsWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 = ?" ) ).execute( 1, 2 );

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 2 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchEqualsOneFieldWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = ?" ) ).execute( 1 );

    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testNoCompositeSearchEqualsWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 = ?" ) ).execute( 1 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", i );
      document.field( "prop2", 1 );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }
  }

  @Test
  public void testCompositeSearchGT()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 > 2" ) ).execute();

    Assert.assertEquals( result.size(), 7 );

    for( int i = 3; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTOneField()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 > 7" ) ).execute();

    Assert.assertEquals( result.size(), 20 );

    for( int i = 8; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearch()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 > 7" ) ).execute();

    Assert.assertEquals( result.size(), 20 );

    for( int i = 8; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchGTWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 > ?" ) ).execute( 1, 2 );

    Assert.assertEquals( result.size(), 7 );

    for( int i = 3; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTOneFieldWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 > ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 20 );

    for( int i = 8; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTOneFieldNoSearchWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 > ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 20 );

    for( int i = 8; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchGTQ()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 >= 2" ) ).execute();

    Assert.assertEquals( result.size(), 8 );

    for( int i = 2; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTQOneField()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 >= 7" ) ).execute();

    Assert.assertEquals( result.size(), 30 );

    for( int i = 7; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearch()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 >= 7" ) ).execute();

    Assert.assertEquals( result.size(), 30 );

    for( int i = 7; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchGTQWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 >= ?" ) ).execute( 1, 2 );

    Assert.assertEquals( result.size(), 8 );

    for( int i = 2; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTQOneFieldWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 >= ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 30 );

    for( int i = 7; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchGTQOneFieldNoSearchWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 >= ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 30 );

    for( int i = 7; i < 10; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchLTQ()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 <= 2" ) ).execute();

    Assert.assertEquals( result.size(), 3 );

    for( int i = 0; i <= 2; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );

  }

  @Test
  public void testCompositeSearchLTQOneField()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 <= 7" ) ).execute();

    Assert.assertEquals( result.size(), 80 );

    for( int i = 0; i <= 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearch()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 <= 7" ) ).execute();

    Assert.assertEquals( result.size(), 80 );

    for( int i = 0; i <= 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchLTQWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 <= ?" ) ).execute( 1, 2 );

    Assert.assertEquals( result.size(), 3 );

    for( int i = 0; i <= 2; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTQOneFieldWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 <= ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 80 );

    for( int i = 0; i <= 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTQOneFieldNoSearchWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 <= ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 80 );

    for( int i = 0; i <= 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }


  @Test
  public void testCompositeSearchLT()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 < 2" ) ).execute();

    Assert.assertEquals( result.size(), 2 );

    for( int i = 0; i < 2; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTOneField()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 < 7" ) ).execute();

    Assert.assertEquals( result.size(), 70 );

    for( int i = 0; i < 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearch()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 < 7" ) ).execute();

    Assert.assertEquals( result.size(), 70 );

    for( int i = 0; i < 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }


  @Test
  public void testCompositeSearchLTWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = ? and prop2 < ?" ) ).execute( 1, 2 );

    Assert.assertEquals( result.size(), 2 );

    for( int i = 0; i < 2; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTOneFieldWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 < ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 70 );

    for( int i = 0; i < 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchLTOneFieldNoSearchWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 < ?" ) ).execute( 7 );

    Assert.assertEquals( result.size(), 70 );

    for( int i = 0; i < 7; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }


  @Test
  public void testCompositeSearchBetween()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 between 1 and 3" ) ).execute();

    Assert.assertEquals( result.size(), 3 );

    for( int i = 1; i <= 3; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }


  @Test
  public void testCompositeSearchBetweenOneField()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 between 1 and 3" ) ).execute();

    Assert.assertEquals( result.size(), 30 );

    for( int i = 1; i <= 3; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearch()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 between 1 and 3" ) ).execute();

    Assert.assertEquals( result.size(), 30 );

    for( int i = 1; i <= 3; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  @Test
  public void testCompositeSearchBetweenWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 between ? and ?" ) ).execute( 1, 3 );

    Assert.assertEquals( result.size(), 3 );

    for( int i = 1; i <= 3; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }


  @Test
  public void testCompositeSearchBetweenOneFieldWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 between ? and ?" ) ).execute( 1, 3 );

    Assert.assertEquals( result.size(), 30 );

    for( int i = 1; i <= 3; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", i );
        document.field( "prop2", j );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testCompositeSearchBetweenOneFieldNoSearchWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop2 between ? and ?" ) ).execute( 1, 3 );

    Assert.assertEquals( result.size(), 30 );

    for( int i = 1; i <= 3; i++ ) {
      for( int j = 0; j < 10; j++ ) {
        final ODocument document = new ODocument();
        document.field( "prop1", j );
        document.field( "prop2", i );

        Assert.assertEquals( containsDocument( result, document ), 1 );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }


  @Test
  public void testSingleSearchEquals()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 = 1" ) ).execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop3" ).intValue(), 1 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchEqualsWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 = ?" ) ).execute( 1 );

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop3" ).intValue(), 1 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }


  @Test
  public void testSingleSearchGT()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 > 90" ) ).execute();

    Assert.assertEquals( result.size(), 9 );

    for( int i = 91; i < 100; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchGTWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 > ?" ) ).execute( 90 );

    Assert.assertEquals( result.size(), 9 );

    for( int i = 91; i < 100; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }


  @Test
  public void testSingleSearchGTQ()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 >= 90" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    for( int i = 90; i < 100; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchGTQWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 >= ?" ) ).execute( 90 );

    Assert.assertEquals( result.size(), 10 );

    for( int i = 90; i < 100; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }


  @Test
  public void testSingleSearchLTQ()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 <= 10" ) ).execute();

    Assert.assertEquals( result.size(), 11 );

    for( int i = 0; i <= 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchLTQWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 <= ?" ) ).execute( 10 );

    Assert.assertEquals( result.size(), 11 );

    for( int i = 0; i <= 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }


  @Test
  public void testSingleSearchLT()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 < 10" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchLTWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 < ?" ) ).execute( 10 );

    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }


  @Test
  public void testSingleSearchBetween()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 between 1 and 10" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    for( int i = 1; i <= 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchBetweenWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 between ? and ?" ) ).execute( 1, 10 );

    Assert.assertEquals( result.size(), 10 );

    for( int i = 1; i <= 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }


  @Test
  public void testSingleSearchIN()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 in [0, 5, 10]" ) ).execute();

    Assert.assertEquals( result.size(), 3 );

    for( int i = 0; i <= 10; i += 5 ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testSingleSearchINWithArgs()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop3 in [?, ?, ?]" ) ).execute( 0, 5, 10 );

    Assert.assertEquals( result.size(), 3 );

    for( int i = 0; i <= 10; i += 5 ) {
      final ODocument document = new ODocument();
      document.field( "prop3", i );
      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testMostSpecificOnesProcessedFirst()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and prop3 = 11" ) )
      .execute();


    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop3" ).intValue(), 11 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testTripleSearch()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage3 = profiler.getCounter( "Query.compositeIndexUsage.3" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage3 == -1 ) {
      oldCompositeIndexUsage3 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and prop4 >= 1" ) )
      .execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop4" ).intValue(), 1 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.3" ), oldCompositeIndexUsage3 + 1 );
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexFirstCase()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 = 1 and prop5 >= 1" ) )
      .execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop5" ).intValue(), 1 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testTripleSearchLastFieldNotInIndexSecondCase()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop4 >= 1" ) )
      .execute();

    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );
      document.field( "prop4", 1 );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testTripleSearchLastFieldInIndex()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage3 = profiler.getCounter( "Query.compositeIndexUsage.3" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage3 == -1 ) {
      oldCompositeIndexUsage3 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop4 = 1" ) )
      .execute();

    Assert.assertEquals( result.size(), 10 );

    for( int i = 0; i < 10; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop1", 1 );
      document.field( "prop2", i );
      document.field( "prop4", 1 );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.3" ), oldCompositeIndexUsage3 + 1 );
  }

  @Test
  public void testTripleSearchLastFieldsCanNotBeMerged()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage3 = profiler.getCounter( "Query.compositeIndexUsage.3" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage3 == -1 ) {
      oldCompositeIndexUsage3 = 0;
    }


    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop6 <= 1 and prop4 < 1" ) )
      .execute();

    Assert.assertEquals( result.size(), 2 );

    for( int i = 0; i < 2; i++ ) {
      final ODocument document = new ODocument();
      document.field( "prop6", i );
      document.field( "prop4", 0 );

      Assert.assertEquals( containsDocument( result, document ), 1 );
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.3" ), oldCompositeIndexUsage3 + 1 );
  }

  @Test
  public void testFullTextIndex()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop7 containstext 'Alice' " ) )
      .execute();

    Assert.assertEquals( result.size(), 20 );

    final ODocument docOne = new ODocument();
    docOne.field( "prop7", "Alice : What is the use of a book, without pictures or conversations?" );
    Assert.assertEquals( containsDocument( result, docOne ), 10 );

    final ODocument docTwo = new ODocument();
    docTwo.field( "prop7", "Alice : If it had grown up, it would have made a dreadfully ugly child; but it makes rather a handsome pig, I think" );
    Assert.assertEquals( containsDocument( result, docTwo ), 10 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
  }

  @Test
  public void testLastFieldNotCompatibleOperator()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2 + 1 = 3" ) ).execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 2 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testEmbeddedMapByKeyIndexReuse()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fEmbeddedMap containskey 'key12'" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    final ODocument document = new ODocument();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put( "key11", 11 );
    embeddedMap.put( "key12", 12 );
    embeddedMap.put( "key13", 13 );
    embeddedMap.put( "key14", 11 );

    document.field( "fEmbeddedMap", embeddedMap );

    Assert.assertEquals( containsDocument( result, document ), 10 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }


  @Test
  public void testEmbeddedMapBySpecificKeyIndexReuse()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where ( fEmbeddedMap containskey 'key12' ) and ( fEmbeddedMap['key12'] = 12 )" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    final ODocument document = new ODocument();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put( "key11", 11 );
    embeddedMap.put( "key12", 12 );
    embeddedMap.put( "key13", 13 );
    embeddedMap.put( "key14", 11 );

    document.field( "fEmbeddedMap", embeddedMap );

    Assert.assertEquals( containsDocument( result, document ), 10 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testEmbeddedMapByValueIndexReuse()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fEmbeddedMap containsvalue 11" ) ).execute();

    Assert.assertEquals( result.size(), 10 );

    final ODocument document = new ODocument();

    final Map<String, Integer> embeddedMap = new HashMap<String, Integer>();

    embeddedMap.put( "key11", 11 );
    embeddedMap.put( "key12", 12 );
    embeddedMap.put( "key13", 13 );
    embeddedMap.put( "key14", 11 );

    document.field( "fEmbeddedMap", embeddedMap );

    Assert.assertEquals( containsDocument( result, document ), 10 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testLinkMapByKeyIndexReuse()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fLinkMap containskey 'key12'" ) ).execute();

    Assert.assertEquals( result.size(), 10 );


    final Map<String, ORID> linkMap = new HashMap<String, ORID>();

    final int clusterId = database.getMetadata().getSchema().getClass( "sqlSelectIndexReuseTestClass" ).getClusterIds()[0];

    linkMap.put( "key11", new ORecordId( clusterId, 1 ) );
    linkMap.put( "key12", new ORecordId( clusterId, 1 ) );
    linkMap.put( "key13", new ORecordId( clusterId, 1 ) );
    linkMap.put( "key14", new ORecordId( clusterId, 1 ) );

    for(final ODocument doc : result ) {
      final Map<String, OIdentifiable> resultLinkMap = (Map<String, OIdentifiable>) doc.field( "fLinkMap");
      
      for(Map.Entry<String, OIdentifiable> mapEntry : resultLinkMap.entrySet() ) {
        final ORID link = (linkMap.get( mapEntry.getKey() )).getIdentity();
        Assert.assertEquals( mapEntry.getValue(), link );
      }
    }
    
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testLinkMapByValueIndexReuse()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final int clusterId = database.getMetadata().getSchema().getClass( "sqlSelectIndexReuseTestClass" ).getClusterIds()[0];
    final ORID ridToSearch = new ORecordId( clusterId, 2 );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fLinkMap containsvalue ?" ) ).execute(ridToSearch);

    Assert.assertEquals( result.size(), 10 );


    final Map<String, ORID> linkMap = new HashMap<String, ORID>();

    linkMap.put( "key21", new ORecordId( clusterId, 2 ) );
    linkMap.put( "key22", new ORecordId( clusterId, 2 ) );
    linkMap.put( "key23", new ORecordId( clusterId, 2 ) );
    linkMap.put( "key24", new ORecordId( clusterId, 2 ) );

    for(final ODocument doc : result ) {
      final Map<String, OIdentifiable> resultLinkMap = (Map<String, OIdentifiable>) doc.field( "fLinkMap");

      for(Map.Entry<String, OIdentifiable> mapEntry : resultLinkMap.entrySet() ) {
        final ORID link = (linkMap.get( mapEntry.getKey() )).getIdentity();
        Assert.assertEquals( mapEntry.getValue(), link );
      }
    }

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testLinkMapByConditionInValueIndexNotReused()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    final int clusterId = database.getMetadata().getSchema().getClass( "sqlSelectIndexReuseTestClass" ).getClusterIds()[0];
    final ORID ridToSearch = new ORecordId( clusterId, 2 );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fLinkMap containsvalue (ftosearch = 'ftosearch')" ) ).execute(ridToSearch);

    Assert.assertEquals( result.size(), 100 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testEmbeddedListIndexReuse() {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fEmbeddedList contains 7" ) ).execute();

    final List<Integer> embeddedList = new ArrayList<Integer>( 3 );
    embeddedList.add( 6 );
    embeddedList.add( 7 );
    embeddedList.add( 8 );

    final ODocument document = new ODocument();
    document.field( "fEmbeddedList", embeddedList );

    Assert.assertEquals( containsDocument( result, document ), 10 );
   
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testLinkListIndexReuse() {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }

    final int clusterId = database.getMetadata().getSchema().getClass( "sqlSelectIndexReuseTestClass" ).getClusterIds()[0];
    final ORID ridToSearch = new ORecordId( clusterId, 1 );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fLinkList contains ?" ) ).execute(ridToSearch);

    Assert.assertEquals( result.size(), 20 );

    final List<ORID> linkListOne = new ArrayList<ORID>( 2 );
    linkListOne.add( new ORecordId( clusterId, 0 ) );
    linkListOne.add( new ORecordId( clusterId, 1 ) );

    final List<ORID> linkListTwo = new ArrayList<ORID>( 2 );
    linkListTwo.add( new ORecordId( clusterId, 1 ) );
    linkListTwo.add( new ORecordId( clusterId, 2 ) );

    int listOneCounter = 0;
    int listTwoCounter = 0;

    for(final ODocument doc : result) {
      final List<OIdentifiable> linkList = doc.field( "fLinkList" );
      
      boolean isContainedInFirstList = true;
      boolean isContainedInSecondList = true;

      for(final OIdentifiable link : linkList) {
        if(!linkListOne.contains( link.getIdentity() ))
          isContainedInFirstList = false;

        if(!linkListTwo.contains( link.getIdentity() ))
          isContainedInSecondList = false;
      }

      if(isContainedInFirstList)
        listOneCounter++;

      if(isContainedInSecondList)
        listTwoCounter++;
    }

    Assert.assertEquals( listOneCounter, 10 );
    Assert.assertEquals( listTwoCounter, 10 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }

  @Test
  public void testLinkListConditionForValueIndexNotReused() {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where fLinkList contains (ftosearch = 'ftosearch')" ) ).execute();

    Assert.assertEquals( result.size(), 100 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 );
  }


  @Test
  public void testNotIndexOperatorFirstCase()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );
    long oldCompositeIndexUsage = profiler.getCounter( "Query.compositeIndexUsage" );
    long oldCompositeIndexUsage2 = profiler.getCounter( "Query.compositeIndexUsage.2" );

    if ( oldIndexUsage == -1 ) {
      oldIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage == -1 ) {
      oldCompositeIndexUsage = 0;
    }
    if ( oldCompositeIndexUsage2 == -1 ) {
      oldCompositeIndexUsage2 = 0;
    }

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where prop1 = 1 and prop2  = 2 and ( prop4 = 3 or prop4 = 1 )" ) ).execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 2 );
    Assert.assertEquals( document.<Integer>field( "prop4" ).intValue(), 1 );
    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage" ), oldCompositeIndexUsage + 1 );
    Assert.assertEquals( profiler.getCounter( "Query.compositeIndexUsage.2" ), oldCompositeIndexUsage2 + 1 );
  }

  @Test
  public void testNotIndexOperatorSecondCase()
  {
    long oldIndexUsage = profiler.getCounter( "Query.indexUsage" );

    final List<ODocument> result = database.command(
      new OSQLSynchQuery<ODocument>( "select * from sqlSelectIndexReuseTestClass where ( prop1 = 1 and prop2 = 2 ) or ( prop4  = 1 and prop6 = 2 )" ) ).execute();

    Assert.assertEquals( result.size(), 1 );

    final ODocument document = result.get( 0 );
    Assert.assertEquals( document.<Integer>field( "prop1" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop2" ).intValue(), 2 );
    Assert.assertEquals( document.<Integer>field( "prop4" ).intValue(), 1 );
    Assert.assertEquals( document.<Integer>field( "prop6" ).intValue(), 2 );

    Assert.assertEquals( profiler.getCounter( "Query.indexUsage" ), oldIndexUsage );
  }

  private int containsDocument( final List<ODocument> docList, final ODocument document )
  {
    int count = 0;
    for( final ODocument docItem : docList ) {
      boolean containsAllFields = true;
      for( final String fieldName : document.fieldNames() ) {
        if ( !document.<Object>field( fieldName ).equals( docItem.<Object>field( fieldName ) ) ) {
          containsAllFields = false;
          break;
        }
      }
      if ( containsAllFields ) {
        count++;
      }
    }
    return count;
  }

  private boolean isRemoteStorage()
  {
    return database.getStorage() instanceof OStorageRemote ||
      database.getStorage() instanceof OStorageRemoteThread;
  }

  private void closeJMXConnector() throws Exception
  {
    if ( isRemoteStorage() ) {
      jmxConnector.close();
    }
  }

  private OProfilerMBean getProfilerInstance() throws Exception
  {
    if ( isRemoteStorage() ) {
      final JMXServiceURL url = new JMXServiceURL( "service:jmx:rmi:///jndi/rmi://localhost:4321/jmxrmi" );
      jmxConnector = JMXConnectorFactory.connect( url, null );
      final MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
      final ObjectName onProfiler = new ObjectName( "OrientDB:type=Profiler" );
      return JMX.newMBeanProxy( mbsc, onProfiler, OProfilerMBean.class, false );
    } else {
      return OProfiler.getInstance();
    }
  }

}

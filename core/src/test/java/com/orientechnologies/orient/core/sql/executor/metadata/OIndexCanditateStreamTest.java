package com.orientechnologies.orient.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OExactIndexStream;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;

public class OIndexCanditateStreamTest extends BaseMemoryDatabase {
  
  @Test
  public void simpleMatch() {
    OClass cl = db.createClass("test");
    OProperty prop = cl.createProperty("name", OType.STRING);
    OIndex index = prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    
    OIndexCandidate candidate =  new OIndexCandidateImpl(index.getName(), Operation.Eq, prop, "a");
    
    List<OIndexStream> streams = candidate.getStreams(new OBasicCommandContext(db), false);
    assertEquals(streams.size(), 1);
    assertTrue(streams.get(0) instanceof OExactIndexStream);
    
  }
  

  @Test
  public void simpleMultiple() {
    OClass cl = db.createClass("test");
    OProperty prop = cl.createProperty("name", OType.STRING);
    OIndex index = prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    
    OIndexCandidate first =  new OIndexCandidateImpl(index.getName(), Operation.Eq, prop, "a");
    OIndexCandidate second =  new OIndexCandidateImpl(index.getName(), Operation.Eq, prop, "a");
    
    
    OMultipleIndexCanditate candidate = new OMultipleIndexCanditate();
    candidate.addCanditate(first);
    candidate.addCanditate(second);
    List<OIndexStream> streams = candidate.getStreams(new OBasicCommandContext(db), false);
    assertEquals(streams.size(), 2);
    assertTrue(streams.get(0) instanceof OExactIndexStream);
    assertTrue(streams.get(1) instanceof OExactIndexStream);
    
  }
  
}

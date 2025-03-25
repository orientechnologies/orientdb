package com.orientechnologies.orient.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OBetweenIndexStream;
import com.orientechnologies.orient.core.sql.executor.OExactIndexStream;
import com.orientechnologies.orient.core.sql.executor.OIndexStream;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class OIndexCanditateStreamTest extends BaseMemoryDatabase {

  @Test
  public void simpleMatch() {
    OClass cl = db.createClass("test");
    OProperty prop = cl.createProperty("name", OType.STRING);
    OIndex index = prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OIndexCandidate candidate =
        new OIndexCandidateOne(
            index.getName(),
            Operation.Eq,
            prop.getName(),
            (ctx, asc) -> Collections.singleton("a"),
            false);

    List<OIndexStream> streams = candidate.getStreams(new OBasicCommandContext(db), false);
    assertEquals(streams.size(), 1);
    assertTrue(streams.get(0) instanceof OExactIndexStream);
  }

  @Test
  public void simpleMultiple() {
    OClass cl = db.createClass("test");
    OProperty prop = cl.createProperty("name", OType.STRING);
    OIndex index = prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OIndexCandidate first =
        new OIndexCandidateOne(
            index.getName(),
            Operation.Eq,
            prop.getName(),
            (ctx, asc) -> Collections.singleton("a"),
            false);
    OIndexCandidate second =
        new OIndexCandidateOne(
            index.getName(),
            Operation.Eq,
            prop.getName(),
            (ctx, asc) -> Collections.singleton("a"),
            false);

    OIndexCanditateAny candidate = new OIndexCanditateAny();
    candidate.addCanditate(first);
    candidate.addCanditate(second);
    List<OIndexStream> streams = candidate.getStreams(new OBasicCommandContext(db), false);
    assertEquals(streams.size(), 2);
    assertTrue(streams.get(0) instanceof OExactIndexStream);
    assertTrue(streams.get(1) instanceof OExactIndexStream);
  }

  @Test
  public void simpleMultipleBetween() {
    OClass cl = db.createClass("test");
    OProperty prop = cl.createProperty("name", OType.STRING);
    OIndex index = prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    OIndexCandidate first =
        new OIndexCandidateOne(
            index.getName(),
            Operation.Le,
            prop.getName(),
            (ctx, asc) -> Collections.singleton("a"),
            false);
    OIndexCandidate second =
        new OIndexCandidateOne(
            index.getName(),
            Operation.Ge,
            prop.getName(),
            (ctx, asc) -> Collections.singleton("a"),
            false);

    OIndexCanditateAny candidate = new OIndexCanditateAny();
    candidate.addCanditate(first);
    candidate.addCanditate(second);
    OBasicCommandContext ctx = new OBasicCommandContext(db);
    OIndexCandidate newCandidate = candidate.normalize(ctx).get();
    List<OIndexStream> streams = newCandidate.getStreams(ctx, false);
    assertEquals(streams.size(), 1);
    assertTrue(streams.get(0) instanceof OBetweenIndexStream);
  }
}

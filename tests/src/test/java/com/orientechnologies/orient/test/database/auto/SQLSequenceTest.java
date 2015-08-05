package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/5/2015
 */
@Test(groups="SqlSequence")
public class SQLSequenceTest extends DocumentDBBaseTest {
  private static final int CACHE_SIZE = 40;
  private static final long FIRST_START = OSequence.DEFAULT_START;
  private static final long SECOND_START = 31;

  @Parameters(value = "url")
  public SQLSequenceTest(@Optional String url) {
    super(url);
  }

  @Test
  public void trivialTest() {
    testSequence("seq1", OSequence.SEQUENCE_TYPE.ORDERED);
    testSequence("seq2", OSequence.SEQUENCE_TYPE.CACHED);
  }

  private void testSequence(String sequenceName, OSequence.SEQUENCE_TYPE sequenceType) {
    OSequenceLibrary sequenceLibrary = database.getMetadata().getSequenceLibrary();

    database.command(new OCommandSQL("CREATE SEQUENCE " + sequenceName + " TYPE " + sequenceType));

    OSequenceException err = null;
    try {
      database.command(new OCommandSQL("CREATE SEQUENCE " + sequenceName + " TYPE " + sequenceType));
    } catch (OSequenceException se) {
      err = se;
    }
    Assert.assertTrue(err == null || err.getMessage().toLowerCase().contains("already exists"),
            "Creating a second " + sequenceType.toString() +
                    " sequences with same name doesn't throw an exception");

    // Doing it twice to check everything works after reset
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(sequenceCurrent(sequenceName), 0L);
      Assert.assertEquals(sequenceNext(sequenceName), 1L);
      Assert.assertEquals(sequenceCurrent(sequenceName), 1L);
      Assert.assertEquals(sequenceNext(sequenceName), 2L);
      Assert.assertEquals(sequenceNext(sequenceName), 3L);
      Assert.assertEquals(sequenceNext(sequenceName), 4L);
      Assert.assertEquals(sequenceCurrent(sequenceName), 4L);
      Assert.assertEquals(sequenceReset(sequenceName), 0L);
    }
  }

  private long sequenceReset(String sequenceName) {
    return sequenceSql(sequenceName, "reset");
  }

  private long sequenceNext(String sequenceName) {
    return sequenceSql(sequenceName, "next");
  }

  private long sequenceCurrent(String sequenceName) {
    return sequenceSql(sequenceName, "current");
  }

  private long sequenceSql(String sequenceName, String cmd) {
    Object ret = database.command(new OCommandSQL("SELECT #" + sequenceName + "." + cmd)).execute();
    return 0;
  }

  @Test
  public void testFree() {
    OSequenceLibrary sequenceManager = database.getMetadata().getSequenceLibrary();

    OSequence seq = sequenceManager.createSequence("seqOrdered", OSequence.SEQUENCE_TYPE.ORDERED, null);

    OSequenceException err = null;
    try {
      sequenceManager.createSequence("seqOrdered", OSequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (OSequenceException se) {
      err = se;
    }
    Assert.assertTrue(err == null || err.getMessage().toLowerCase().contains("already exists"),
            "Creating two ordered sequences with same name doesn't throw an exception");

    OSequence seqSame = sequenceManager.getSequence("seqOrdered");
    Assert.assertEquals(seqSame, seq);

    testUsage(seq, FIRST_START);

    //
    seq.updateParams(new OSequence.CreateParams().setStart(SECOND_START).setCacheSize(13));
    testUsage(seq, SECOND_START);
  }

  private void testUsage(OSequence seq, long reset) {
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(seq.reset(), reset);
      Assert.assertEquals(seq.current(), reset);
      Assert.assertEquals(seq.next(), reset + 1L);
      Assert.assertEquals(seq.current(), reset + 1L);
      Assert.assertEquals(seq.next(), reset + 2L);
      Assert.assertEquals(seq.next(), reset + 3L);
      Assert.assertEquals(seq.next(), reset + 4L);
      Assert.assertEquals(seq.current(), reset + 4L);
      Assert.assertEquals(seq.reset(), reset);
    }
  }
}
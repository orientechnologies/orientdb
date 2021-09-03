package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequence.SEQUENCE_TYPE;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/2/2015
 */
@Test(groups = "sequence")
public class SequenceTest extends DocumentDBBaseTest {
  private static final int CACHE_SIZE = 40;
  private static final long FIRST_START = OSequence.DEFAULT_START;
  private static final long SECOND_START = 31;

  @Parameters(value = "url")
  public SequenceTest(@Optional String url) {
    super(url);
  }

  @Test
  public void trivialTest() throws ExecutionException, InterruptedException {
    testSequence("seq1", SEQUENCE_TYPE.ORDERED);
    testSequence("seq2", SEQUENCE_TYPE.CACHED);
  }

  private void testSequence(String sequenceName, SEQUENCE_TYPE sequenceType)
      throws ExecutionException, InterruptedException {
    OSequenceLibrary sequenceLibrary = database.getMetadata().getSequenceLibrary();

    OSequence seq = sequenceLibrary.createSequence(sequenceName, sequenceType, null);

    OSequenceException err = null;
    try {
      sequenceLibrary.createSequence(sequenceName, sequenceType, null);
    } catch (OSequenceException se) {
      err = se;
    }
    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating a second "
            + sequenceType.toString()
            + " sequences with same name doesn't throw an exception");

    OSequence seqSame = sequenceLibrary.getSequence(sequenceName);
    Assert.assertEquals(seqSame, seq);

    // Doing it twice to check everything works after reset
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(seq.next(), 1L);
      Assert.assertEquals(seq.current(), 1L);
      Assert.assertEquals(seq.next(), 2L);
      Assert.assertEquals(seq.next(), 3L);
      Assert.assertEquals(seq.next(), 4L);
      Assert.assertEquals(seq.current(), 4L);
      Assert.assertEquals(seq.reset(), 0L);
    }
  }

  @Test
  public void testOrdered() throws ExecutionException, InterruptedException {
    OSequenceLibrary sequenceManager = database.getMetadata().getSequenceLibrary();

    OSequence seq = sequenceManager.createSequence("seqOrdered", SEQUENCE_TYPE.ORDERED, null);

    OSequenceException err = null;
    try {
      sequenceManager.createSequence("seqOrdered", SEQUENCE_TYPE.ORDERED, null);
    } catch (OSequenceException se) {
      err = se;
    }
    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating two ordered sequences with same name doesn't throw an exception");

    OSequence seqSame = sequenceManager.getSequence("seqOrdered");
    Assert.assertEquals(seqSame, seq);

    testUsage(seq, FIRST_START);

    //
    seq.updateParams(new OSequence.CreateParams().setStart(SECOND_START).setCacheSize(13));
    testUsage(seq, SECOND_START);
  }

  private void testUsage(OSequence seq, long reset)
      throws ExecutionException, InterruptedException {
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

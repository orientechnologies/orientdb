package com.orientechnologies.orient.core.metadata.sequence;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSequenceException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 22/04/2016.
 */
public class OSequenceTest {

  private ODatabaseDocument db;

  @Rule
  public ExternalResource resource = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      db = new ODatabaseDocumentTx("memory:" + OSequenceTest.class.getName());
      db.create();
    }

    @Override
    protected void after() {
      db.drop();
    }

  };
  private OSequenceLibrary sequences;

  @Before
  public void setUp() throws Exception {

    sequences = db.getMetadata().getSequenceLibrary();

  }

  @Test
  public void shouldCreateSeqWithGivenAttribute() {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams().setDefaults());

    assertThat(sequences.getSequenceCount()).isEqualTo(1);
    assertThat(sequences.getSequenceNames()).contains("MYSEQ");


    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.getSequenceType()).isEqualTo(OSequence.SEQUENCE_TYPE.ORDERED);
    assertThat(myseq.getMaxRetry()).isEqualTo(100);

  }

  @Test
  public void shouldGivesValuesOrdered() throws Exception {

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams().setDefaults());
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.current()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.current()).isEqualTo(2);

  }

  @Test
  public void shouldGivesValuesWithIncrement() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setDefaults().setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(60);

  }

  @Test
  public void shouldCache() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setDefaults().setCacheSize(100).setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq).isInstanceOf(OSequenceCached.class);
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(60);
    assertThat(myseq.current()).isEqualTo(60);
    assertThat(myseq.next()).isEqualTo(90);
    assertThat(myseq.current()).isEqualTo(90);
    assertThat(myseq.next()).isEqualTo(120);
    assertThat(myseq.current()).isEqualTo(120);

  }

  @Test(expected = OSequenceException.class)
  public void shouldThrowExceptionOnDuplicateSeqDefinition() throws Exception {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);

  }

  @Test
  public void shouldDropSequence() throws Exception {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
    sequences.dropSequence("MYSEQ");
    assertThat(sequences.getSequenceCount()).isEqualTo(0);

    //IDEMPOTENT
    sequences.dropSequence("MYSEQ");
    assertThat(sequences.getSequenceCount()).isEqualTo(0);
  }


  @Test
  public void testCreateSequenceWithoutExplicitDefaults() throws Exception {
    // issue #6484
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
  }

}

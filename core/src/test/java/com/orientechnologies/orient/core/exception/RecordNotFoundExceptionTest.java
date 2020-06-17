package com.orientechnologies.orient.core.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.id.ORecordId;
import org.junit.Test;

/** Created by tglman on 22/02/17. */
public class RecordNotFoundExceptionTest {

  @Test
  public void simpleExceptionCopyTest() {
    ORecordNotFoundException ex = new ORecordNotFoundException(new ORecordId(1, 2));
    ORecordNotFoundException ex1 = new ORecordNotFoundException(ex);
    assertNotNull(ex1.getRid());
    assertEquals(ex1.getRid().getClusterId(), 1);
    assertEquals(ex1.getRid().getClusterPosition(), 2);
  }
}

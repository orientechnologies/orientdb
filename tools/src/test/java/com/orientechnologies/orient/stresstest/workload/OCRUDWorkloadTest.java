package com.orientechnologies.orient.stresstest.workload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class OCRUDWorkloadTest {

  @Test
  public void testParsing() throws Exception {

    try {
      new OCRUDWorkload().parseParameters("");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(OCRUDWorkload.INVALID_FORM_MESSAGE));
    }

    try {
      new OCRUDWorkload().parseParameters("crd");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(OCRUDWorkload.INVALID_FORM_MESSAGE));
    }

    try {
      new OCRUDWorkload().parseParameters("c0r0u0d0");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(OCRUDWorkload.INVALID_FORM_MESSAGE));
    }

    try {
      new OCRUDWorkload().parseParameters("c1r1u1d1p1");
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(OCRUDWorkload.INVALID_FORM_MESSAGE));
    }

    OCRUDWorkload workload = new OCRUDWorkload();
    workload.parseParameters("C1R1U1D1S1");
    assertEquals(1, workload.getCreates());
    assertEquals(1, workload.getReads());
    assertEquals(1, workload.getScans());
    assertEquals(1, workload.getUpdates());
    assertEquals(1, workload.getDeletes());

    workload = new OCRUDWorkload();
    workload.parseParameters("c1r1u1d1");
    assertEquals(1, workload.getCreates());
    assertEquals(1, workload.getReads());
    assertEquals(1, workload.getUpdates());
    assertEquals(1, workload.getDeletes());

    workload = new OCRUDWorkload();
    workload.parseParameters("c100r99u01d99");
    assertEquals(100, workload.getCreates());
    assertEquals(99, workload.getReads());
    assertEquals(1, workload.getUpdates());
    assertEquals(99, workload.getDeletes());

    workload = new OCRUDWorkload();
    workload.parseParameters("d99u01r099c100");
    assertEquals(100, workload.getCreates());
    assertEquals(99, workload.getReads());
    assertEquals(1, workload.getUpdates());
    assertEquals(99, workload.getDeletes());
    assertEquals(0, workload.getScans());
  }
}

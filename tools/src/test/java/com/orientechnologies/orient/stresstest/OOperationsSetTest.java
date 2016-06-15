package com.orientechnologies.orient.stresstest;

import com.orientechnologies.orient.stresstest.operations.OOperationType;
import com.orientechnologies.orient.stresstest.operations.OOperationsSet;
import com.orientechnologies.orient.stresstest.util.OErrorMessages;

import static com.orientechnologies.orient.stresstest.util.OErrorMessages.OPERATION_SET_INVALID_FORM_MESSAGE;
import static com.orientechnologies.orient.stresstest.util.OErrorMessages.OPERATION_SET_SHOULD_CONTAIN_ALL_MESSAGE;
import static org.junit.Assert.*;

public class OOperationsSetTest {

    @org.junit.Test
    public void testEmptyOperationsSet() throws Exception {

        try {
            new OOperationsSet("");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(OPERATION_SET_SHOULD_CONTAIN_ALL_MESSAGE));
        }

        try {
            new OOperationsSet("crd");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(OPERATION_SET_SHOULD_CONTAIN_ALL_MESSAGE));
        }

        try {
            new OOperationsSet("c1r1u1d1p1");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(OPERATION_SET_INVALID_FORM_MESSAGE));
        }

        try {
            new OOperationsSet("c1r1u1d");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(OPERATION_SET_INVALID_FORM_MESSAGE));
        }

        try {
            new OOperationsSet("c1r1ud1");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(OPERATION_SET_INVALID_FORM_MESSAGE));
        }

        try {
            new OOperationsSet("c100r1u01d9999");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_DELETES_GT_CREATES, 9999, 100)));
        }

        try {
            new OOperationsSet("c100r101u1d99");
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_READS_GT_CREATES, 101, 100)));
        }


        OOperationsSet set = new OOperationsSet("C1R1U1D1");
        assertEquals(1, set.getNumber(OOperationType.CREATE));
        assertEquals(1, set.getNumber(OOperationType.READ));
        assertEquals(1, set.getNumber(OOperationType.UPDATE));
        assertEquals(1, set.getNumber(OOperationType.DELETE));

        set = new OOperationsSet("c1r1u1d1");
        assertEquals(1, set.getNumber(OOperationType.CREATE));
        assertEquals(1, set.getNumber(OOperationType.READ));
        assertEquals(1, set.getNumber(OOperationType.UPDATE));
        assertEquals(1, set.getNumber(OOperationType.DELETE));

        set = new OOperationsSet("c100r99u01d99");
        assertEquals(100, set.getNumber(OOperationType.CREATE));
        assertEquals(99, set.getNumber(OOperationType.READ));
        assertEquals(1, set.getNumber(OOperationType.UPDATE));
        assertEquals(99, set.getNumber(OOperationType.DELETE));
    }
}
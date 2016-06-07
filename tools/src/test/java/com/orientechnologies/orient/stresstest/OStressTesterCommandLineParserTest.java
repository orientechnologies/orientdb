package com.orientechnologies.orient.stresstest;

import com.orientechnologies.orient.stresstest.util.OErrorMessages;
import org.junit.Test;

import static org.junit.Assert.*;

public class OStressTesterCommandLineParserTest {

    @Test
    public void testCommandLineArgs() throws Exception {
        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{""});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_OPTION, "")));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-i foo"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_EXPECTED_VALUE, "-i foo")));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-i"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_EXPECTED_VALUE, "-i")));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-t", "10", "-n"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_EXPECTED_VALUE, "-n")));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-m", "foo"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_MODE, "foo")));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-m", "remote"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(OErrorMessages.COMMAND_LINE_PARSER_MISSING_REMOTE_IP));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-m", "memory", "-x", "4", "-n", "10", "-t", "2", "-s", "C60R60U60D60"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_TX_GREATER_THAN_CREATES, 4, 3)));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-n", "10"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(OErrorMessages.COMMAND_LINE_PARSER_MODE_PARAM_MANDATORY));
        }

        OStressTester stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100", "--root-password", "foo", "-m", "plocal"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals("foo", stressTester.getPassword());
        assertEquals(OMode.PLOCAL, stressTester.getDatabaseIdentifier().getMode());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-m", "memory", "--root-password", "foo"});
        assertEquals("foo", stressTester.getPassword());
        assertEquals(OMode.MEMORY, stressTester.getDatabaseIdentifier().getMode());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100", "-t", "4", "--root-password", "foo", "-m", "plocal"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertNull(stressTester.getDatabaseIdentifier().getRemoteIp());
        assertEquals(2424, stressTester.getDatabaseIdentifier().getRemotePort());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-m","remote", "--remote-ip", "127.0.0.1", "--root-password", "foo"});
        assertEquals("foo", stressTester.getPassword());
        assertEquals("127.0.0.1", stressTester.getDatabaseIdentifier().getRemoteIp());
        assertEquals(2424, stressTester.getDatabaseIdentifier().getRemotePort());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-m","remote", "--remote-ip", "127.0.0.1", "--root-password", "foo", "--remote-port", "1025"});
        assertEquals("foo", stressTester.getPassword());
        assertEquals("127.0.0.1", stressTester.getDatabaseIdentifier().getRemoteIp());
        assertEquals(1025, stressTester.getDatabaseIdentifier().getRemotePort());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100","-t","4","-m","plocal", "--root-password", "foo"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertEquals(com.orientechnologies.orient.stresstest.OMode.PLOCAL, stressTester.getMode());
        assertEquals("foo", stressTester.getPassword());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100","-t","4","-m","plocal", "--root-password", "foo", "-x", "12"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertEquals(com.orientechnologies.orient.stresstest.OMode.PLOCAL, stressTester.getMode());
        assertEquals("foo", stressTester.getPassword());
        assertEquals(12, stressTester.getTransactionsNumber());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100","-t","4","-m","plocal","-s","c1r1u1d1", "--root-password", "foo"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertEquals(com.orientechnologies.orient.stresstest.OMode.PLOCAL, stressTester.getMode());
        assertEquals("foo", stressTester.getPassword());
    }
}
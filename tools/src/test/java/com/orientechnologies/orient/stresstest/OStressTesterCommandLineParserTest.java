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
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-i"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_EXPECTED_VALUE, "-i")));
        }

        try {
            OStressTesterCommandLineParser.getStressTester(new String[]{"-m", "foo"});
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains(String.format(OErrorMessages.COMMAND_LINE_PARSER_INVALID_MODE, "foo")));
        }

        OStressTester stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100", "-p", "foo"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals("foo", stressTester.getPassword());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100", "-t", "4", "-p", "foo"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertEquals("foo", stressTester.getPassword());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100","-t","4","-m","plocal", "-p", "foo"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertEquals(com.orientechnologies.orient.stresstest.OMode.PLOCAL, stressTester.getMode());
        assertEquals("foo", stressTester.getPassword());

        stressTester = OStressTesterCommandLineParser.getStressTester(new String[]{"-n","100","-t","4","-m","plocal","-s","c1r1u1d1", "-p", "foo"});
        assertEquals(100, stressTester.getIterationsNumber());
        assertEquals(4, stressTester.getThreadsNumber());
        assertEquals(com.orientechnologies.orient.stresstest.OMode.PLOCAL, stressTester.getMode());
        assertEquals("foo", stressTester.getPassword());
    }
}
package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class OLogTransformerTest extends ETLBaseTest {

    private final PrintStream OUT = System.out;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(output, true));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(OUT);
    }

    @Test
    public void testGetConfiguration() throws Exception {

    }

    @Test
    public void testConfigure() throws Exception {

    }

    @Test
    public void testGetName() throws Exception {

    }

    @Test
    public void testPrefix() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, true));
        String cfgJson = "{source: { content: { value: 'id,postId,text\n1,,Hello'} }, extractor : { row : {} }, transformers : [{ csv : {} },{ log : {prefix:'-> '}}], loader : { test: {} } }";
        process(cfgJson);
        List<ODocument> res = getResult();
        ODocument doc = res.get(0);
        assertEquals("-> ", output.toString());

    }

    @Test
    public void testPostfix() throws Exception {
        String cfgJson = "{source: { content: { value: 'id,postId,text\n1,,Hello'} }, extractor : { row : {} }, transformers : [{ csv : {} },{ log : {postfix:'-> '}}], loader : { test: {} } }";
        process(cfgJson);
        List<ODocument> res = getResult();
        ODocument doc = res.get(0);
        assertEquals("-> ", output.toString());
    }


}
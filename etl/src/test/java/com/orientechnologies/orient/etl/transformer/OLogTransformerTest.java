package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.ETLBaseTest;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OLogTransformerTest extends ETLBaseTest {

    private final PrintStream OUT = System.out;

    @Test
    public void testPrefix() throws Exception {
        ByteArrayOutputStream output = getByteArrayOutputStream();
        String cfgJson = "{source: { content: { value: 'id,text\n1,Hello\n2,Bye'} }, extractor : { csv: {} }, transformers : [{ log : {prefix:'-> '}}], loader : { test: {} } }";
        process(cfgJson);
        List<ODocument> res = getResult();
        ODocument doc = res.get(0);
        String[] stringList = output.toString().split("\n");
        assertEquals("[1:log] INFO -> {id:1,text:Hello}", stringList[1]);
        assertEquals("[2:log] INFO -> {id:2,text:Bye}", stringList[2]);
    }

    @Test
    public void testPostfix() throws Exception {
        ByteArrayOutputStream output = getByteArrayOutputStream();
        String cfgJson = "{source: { content: { value: 'id,text\n1,Hello\n2,Bye'} }, extractor : { csv : {} }, transformers : [{ log : {postfix:'-> '}}], loader : { test: {} } }";
        process(cfgJson);
        List<ODocument> res = getResult();
        ODocument doc = res.get(0);
        String[] stringList = output.toString().split("\n");

        assertEquals("[1:log] INFO {id:1,text:Hello}-> ", stringList[1]);
        assertEquals("[2:log] INFO {id:2,text:Bye}-> ", stringList[2]);
    }

    private ByteArrayOutputStream getByteArrayOutputStream() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, true));
        return output;
    }

}
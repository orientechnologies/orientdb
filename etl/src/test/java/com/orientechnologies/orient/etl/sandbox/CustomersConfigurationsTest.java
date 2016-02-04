package com.orientechnologies.orient.etl.sandbox;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by frank on 26/10/2015.
 */
public class CustomersConfigurationsTest extends OETLBaseTest {

  @Test
  @Ignore
  public void testConfig() {

    String config =
        "{'config' : {'log': 'DEBUG'} ,   'source': { 'file': { 'path': './test.csv' } },\n" + "    'extractor': { 'csv': { } },\n"
        + "    'transformers': [\n"
        + "        { 'command': { 'command': 'CREATE EDGE E FROM ${input.rid1} TO ${input.rid2}' } }\n" + "    ],\n"
        + "    'loader': {\n" + "        'orientdb': {\n" + "            'dbURL': 'memory:ETLBaseTest',\n"
        + "            'dbType': 'graph',\n" + "            'dbAutoCreate': true,\n"
        + "            'dbAutoCreateProperties': true,\n" + "            'tx': false,\n" + "            'txUseLog': false,\n"
        + "            'wal': true, " + "            'useLightWeightEdges': true,\n" + "            'classes': [\n"
        + "                { 'name': 'IsFileOfFunE', 'extends': 'E' },\n" + "            ]\n" + "        }\n" + "    }\n" + "}";

    process(config, new OBasicCommandContext());

  }
}

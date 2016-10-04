package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Created by frank on 21/09/2016.
 */
public class OCommandTransformerTest extends OETLBaseTest {

  @Test
  public void shouldAllowSingleQuoteInsideFieldValue() {
    //Data contains a field with single quote: Miner O'Greedy
    // so the query must be enclosed inside double quote "
    process("{\n"
        + "  'config': {\n"
        + "    'log': 'INFO'\n"
        + "  },\n"
        + "  'source': {\n"
        + "    'content': {\n"
        + "      'value': \"name,surname\n Jay, Miner O'Greedy \n Jay, Miner O'Greedy   \"\n"
        + "    }\n"
        + "  },\n"
        + "  'transformers': [\n"
        + "    {\n"
        + "      'command': {\n"
        + "       'log': 'INFO',\n"
        + "       'output': 'previous',\n"
        + "       'language': 'sql',\n"
        + "        'command': \"SELECT name FROM Person WHERE surname= \"={eval('$input.surname')}\"\"\n"
        + "      }\n"
        + "    },\n"
        + "  {vertex: {class:'Person', skipDuplicates:false}} "
        + "],"
        + "  'extractor': {\n"
        + "    'csv': {}\n"
        + "  },\n"
        + "  'loader': {\n"
        + "      'orientdb': {\n"
        //        + "       'log': 'DEBUG',\n"

        + "        'dbURL': 'memory:OETLBaseTest',\n"
        + "        'dbType': 'graph',\n"
        + "        'useLightweightEdges': false ,\n"
        + "         \"classes\": [\n"
        + "        {\"name\":\"Person\", \"extends\": \"V\" }"
        + "      ]"
        + "      }\n"
        + "    }\n"
        + "}");

    assertThat(graph.countVertices("Person")).isEqualTo(2);
  }
}

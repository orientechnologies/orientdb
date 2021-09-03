package com.orientechnologies.orient.client.remote.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.common.exception.OErrorCode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** Created by tglman on 13/06/17. */
public class ORemoteErrorMessageTest {

  @Test
  public void testReadWriteErrorMessage() throws IOException {
    MockChannel channel = new MockChannel();
    Map<String, String> messages = new HashMap<>();
    messages.put("one", "two");
    OError37Response response =
        new OError37Response(OErrorCode.GENERIC_ERROR, 10, messages, "some".getBytes());
    response.write(channel, 0, null);
    channel.close();
    OError37Response readResponse = new OError37Response();
    readResponse.read(channel, null);

    assertEquals(readResponse.getCode(), OErrorCode.GENERIC_ERROR);
    assertEquals(readResponse.getErrorIdentifier(), 10);
    assertNotNull(readResponse.getMessages());
    assertEquals(readResponse.getMessages().get("one"), "two");
    assertEquals(new String(readResponse.getVerbose()), "some");
  }
}

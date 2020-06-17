/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.client.remote.message.sequence;

import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/** @author marko */
public class OSequenceActionRequestTest {

  @Test
  public void testSerializeDeserialize() {
    OSequence.CreateParams params = new OSequence.CreateParams().setLimitValue(123l);
    OSequenceAction action =
        new OSequenceAction(
            OSequenceAction.CREATE, "testName", params, OSequence.SEQUENCE_TYPE.ORDERED);
    OSequenceActionRequest request = new OSequenceActionRequest(action);
    ByteArrayOutputStream arrayOutput = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(arrayOutput);
    try {
      request.serialize(out);
      arrayOutput.flush();
      byte[] bytes = arrayOutput.toByteArray();
      arrayOutput.close();

      ByteArrayInputStream arrayInput = new ByteArrayInputStream(bytes);
      DataInput in = new DataInputStream(arrayInput);
      OSequenceActionRequest newRequest = new OSequenceActionRequest();
      newRequest.deserialize(in);

      Assert.assertEquals(newRequest.getAction().getActionType(), action.getActionType());
      Assert.assertEquals(newRequest.getAction().getSequenceName(), action.getSequenceName());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getCacheSize(),
          action.getParameters().getCacheSize());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getIncrement(),
          action.getParameters().getIncrement());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getLimitValue(),
          action.getParameters().getLimitValue());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getOrderType(),
          action.getParameters().getOrderType());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getRecyclable(),
          action.getParameters().getRecyclable());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getStart(), action.getParameters().getStart());
      Assert.assertEquals(
          newRequest.getAction().getParameters().getCurrentValue(),
          action.getParameters().getCurrentValue());
    } catch (IOException exc) {
      Assert.assertTrue(false);
    }
  }
}

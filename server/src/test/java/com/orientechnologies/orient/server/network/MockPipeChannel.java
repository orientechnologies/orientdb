package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/** Created by tglman on 10/05/17. */
public class MockPipeChannel extends OChannelBinary {

  public MockPipeChannel(InputStream in, OutputStream out) throws IOException {
    super(new Socket(), new OContextConfiguration());
    this.in = new DataInputStream(in);
    this.out = new DataOutputStream(out);
  }
}

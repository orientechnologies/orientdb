package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.io.*;
import java.net.Socket;
import java.nio.channels.Pipe;

/**
 * Created by tglman on 10/05/17.
 */
public class MockPipeChannel extends OChannelBinary {

  public MockPipeChannel(InputStream in, OutputStream out) throws IOException {
    super(new Socket(), new OContextConfiguration());
    this.in = new DataInputStream(in);
    this.out = new DataOutputStream(out);
  }
}

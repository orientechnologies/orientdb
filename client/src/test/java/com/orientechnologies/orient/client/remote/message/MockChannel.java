package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.io.*;
import java.net.Socket;

/**
 * Created by luigidellaquila on 14/12/16.
 */
public class  MockChannel extends OChannelBinary {

  private final ByteArrayOutputStream byteOut;

  public MockChannel() throws IOException {
    super(new Socket(), new OContextConfiguration());

    this.byteOut = new ByteArrayOutputStream();
    this.out = new DataOutputStream(byteOut);

  }

  @Override
  public void close() {
    this.in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
  }
}

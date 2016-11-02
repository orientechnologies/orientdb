package com.orientechnologies.orient.client.remote.message;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OImportRequest implements OBinaryRequest<OImportResponse> {
  private InputStream inputStream;
  private String      options;
  private String      name;
  private String      imporPath;

  public OImportRequest(InputStream inputStream, String options, String name) {
    this.inputStream = inputStream;
    this.options = options;
    this.name = name;
  }

  public OImportRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(options);
    network.writeString(name);
    byte[] buffer = new byte[1024];
    int size;
    while ((size = inputStream.read(buffer)) > 0) {
      network.writeBytes(buffer, size);
    }
    network.writeBytes(null);

  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    options = channel.readString();
    name = channel.readString();
    File file = File.createTempFile("import", name);
    FileOutputStream output = new FileOutputStream(file);
    byte[] bytes;
    while ((bytes = channel.readBytes()) != null) {
      output.write(bytes);
    }
    output.close();

    // This may not be closed, to double check
    imporPath = file.getAbsolutePath();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DB_IMPORT;
  }

  @Override
  public String getDescription() {
    return "Import Database";
  }

  public String getImporPath() {
    return imporPath;
  }

  public String getName() {
    return name;
  }

  public String getOptions() {
    return options;
  }

  @Override
  public OImportResponse createResponse() {
    return new OImportResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeImport(this);
  }

}
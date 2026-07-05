package com.orientechnologies.orient.core.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public enum ONodeRole {
  Main,
  Replica;

  public void writeNetwork(DataOutput output) throws IOException {
    output.writeInt(this.ordinal());
  }

  public static ONodeRole readNetwork(DataInput input) throws IOException {
    int ord = input.readInt();
    return ONodeRole.values()[ord];
  }

  public static ONodeRole fromString(String role) {
    return switch (role.toLowerCase()) {
      case "main" -> ONodeRole.Main;
      case "replica" -> ONodeRole.Replica;

      default -> throw new IllegalArgumentException("Unexpected value: " + role);
    };
  }
}

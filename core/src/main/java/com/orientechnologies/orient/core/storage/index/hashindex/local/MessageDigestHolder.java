package com.orientechnologies.orient.core.storage.index.hashindex.local;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageDigestHolder {
  private static final MessageDigestHolder INSTANCE = new MessageDigestHolder();

  private final ThreadLocal<MessageDigest> messageDigest =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException("SHA-256 algorithm is not implemented", e);
            }
          });

  public static MessageDigestHolder instance() {
    return INSTANCE;
  }

  public MessageDigest get() {
    return messageDigest.get();
  }
}

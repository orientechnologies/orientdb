package com.orientechnologies.nio;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/20/13
 */
public class GeneralCLibrary implements CLibrary {
  public static native Pointer memmove(Pointer dest, Pointer src, NativeLong len);

  static {
    Native.register(Platform.C_LIBRARY_NAME);
  }

  @Override
  public void memoryMove(long src, long dest, long len) {
    memmove(new Pointer(dest), new Pointer(src), new NativeLong(len));
  }
}

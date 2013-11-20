package com.orientechnologies.nio;

import com.orientechnologies.common.log.OLogManager;
import com.sun.jna.Function;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 11/20/13
 */
public class AIXCLibrary implements CLibrary {
  private static final Function memmove;
  private static final Function bcopy;

  static {
    Function memmoveFc;
    try {
      memmoveFc = Function.getFunction(Platform.C_LIBRARY_NAME, "memmove");
    } catch (UnsatisfiedLinkError linkError) {
      memmoveFc = null;
    }

    Function bcopyFc;
    try {
      bcopyFc = Function.getFunction(Platform.C_LIBRARY_NAME, "bcopy");
    } catch (UnsatisfiedLinkError linkError) {
      bcopyFc = null;
    }

    memmove = memmoveFc;
    bcopy = bcopyFc;
    OLogManager.instance().debug(CLibrary.class, "Following c library functions were found memmove : %s , bcopy : %s.",
        memmoveFc != null ? "yes" : "no", bcopyFc != null ? "yes" : "no");
  }

  public void memoryMove(long src, long dest, long len) {
    final Pointer srcPointer = new Pointer(src);
    final Pointer destPointer = new Pointer(dest);

    if (memmove != null)
      memmove.invoke(Pointer.class, new Object[] { destPointer, srcPointer, new NativeLong(len) });
    else if (bcopy != null)
      bcopy.invokeVoid(new Object[] { srcPointer, destPointer, new NativeLong(len) });
    else {
      if (src > dest)
        for (long n = 0; n < len; n++)
          destPointer.setByte(n, srcPointer.getByte(n));
      else
        for (long n = len - 1; n >= 0; n--)
          destPointer.setByte(n, srcPointer.getByte(n));
    }
  }
}

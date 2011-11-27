package com.orientechnologies.common.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.orientechnologies.common.exception.OException;

public class OLogManager {

	private boolean										debug					= true;
	private boolean										info					= true;
	private boolean										warn					= true;
	private boolean										error					= true;
	private Level											minimumLevel	= Level.SEVERE;

	private static final OLogManager	instance			= new OLogManager();

	protected OLogManager() {
	}

	public void setConsoleLevel(final String iLevel) {
		setLevel(iLevel, ConsoleHandler.class);
	}

	public void setFileLevel(final String iLevel) {
		setLevel(iLevel, FileHandler.class);
	}

	public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException,
			final Object... iAdditionalArgs) {
		if (iMessage != null) {
			final Logger log = iRequester != null ? Logger.getLogger(iRequester.getClass().getName()) : Logger.getLogger("");
			if (log.isLoggable(iLevel)) {
				// ENCODE OF SPECIAL FORMAT CHAR '%' IF ANY
				if (iMessage.contains("%"))
					iMessage = iMessage.replaceAll("%", "%%");
				final String msg = String.format(iMessage, iAdditionalArgs);
				if (iException != null)
					log.log(iLevel, msg, iException);
				else
					log.log(iLevel, msg, iAdditionalArgs);
			}
		}
	}

	public void debug(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		if (isDebugEnabled())
			log(iRequester, Level.FINE, iMessage, null, iAdditionalArgs);
	}

	public void debug(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isDebugEnabled())
			log(iRequester, Level.FINE, iMessage, iException, iAdditionalArgs);
	}

	public void debug(final Object iRequester, final String iMessage, final Throwable iException,
			final Class<? extends OException> iExceptionClass, final Object... iAdditionalArgs) {
		debug(iRequester, iMessage, iException, iAdditionalArgs);

		try {
			throw iExceptionClass.getConstructor(String.class, Throwable.class).newInstance(iMessage, iException);
		} catch (NoSuchMethodException e) {
		} catch (IllegalArgumentException e) {
		} catch (SecurityException e) {
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		} catch (InvocationTargetException e) {
		}
	}

	public void info(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		if (isInfoEnabled())
			log(iRequester, Level.INFO, iMessage, null, iAdditionalArgs);
	}

	public void info(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isInfoEnabled())
			log(iRequester, Level.INFO, iMessage, iException, iAdditionalArgs);
	}

	public void warn(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		if (isWarnEnabled())
			log(iRequester, Level.WARNING, iMessage, null, iAdditionalArgs);
	}

	public void warn(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isWarnEnabled())
			log(iRequester, Level.WARNING, iMessage, iException, iAdditionalArgs);
	}

	public void error(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		log(iRequester, Level.SEVERE, iMessage, null, iAdditionalArgs);
	}

	public void error(final Object iRequester, final String iMessage, final Throwable iException, final Object... iAdditionalArgs) {
		if (isErrorEnabled())
			log(iRequester, Level.SEVERE, iMessage, iException, iAdditionalArgs);
	}

	public void config(final Object iRequester, final String iMessage, final Object... iAdditionalArgs) {
		log(iRequester, Level.CONFIG, iMessage, null, iAdditionalArgs);
	}

	public void error(final Object iRequester, final String iMessage, final Throwable iException,
			final Class<? extends OException> iExceptionClass, final Object... iAdditionalArgs) {
		error(iRequester, iMessage, iException, iAdditionalArgs);

		final String msg = String.format(iMessage, iAdditionalArgs);

		try {
			throw iExceptionClass.getConstructor(String.class, Throwable.class).newInstance(msg, iException);
		} catch (NoSuchMethodException e) {
		} catch (IllegalArgumentException e) {
		} catch (SecurityException e) {
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		} catch (InvocationTargetException e) {
		}
	}

	public void error(final Object iRequester, final String iMessage, final Class<? extends OException> iExceptionClass) {
		error(iRequester, iMessage, (Throwable) null);

		try {
			throw iExceptionClass.getConstructor(String.class).newInstance(iMessage);
		} catch (IllegalArgumentException e) {
		} catch (SecurityException e) {
		} catch (InstantiationException e) {
		} catch (IllegalAccessException e) {
		} catch (InvocationTargetException e) {
		} catch (NoSuchMethodException e) {
		}
	}

	@SuppressWarnings("unchecked")
	public void exception(final String iMessage, final Exception iNestedException, final Class<? extends OException> iExceptionClass,
			final Object... iAdditionalArgs) throws OException {
		if (iMessage == null)
			return;

		// FORMAT THE MESSAGE
		String msg = String.format(iMessage, iAdditionalArgs);

		Constructor<OException> c;
		OException exceptionToThrow = null;
		try {
			if (iNestedException != null) {
				c = (Constructor<OException>) iExceptionClass.getConstructor(String.class, Throwable.class);
				exceptionToThrow = c.newInstance(msg, iNestedException);
			}
		} catch (Exception e) {
		}

		if (exceptionToThrow == null)
			try {
				c = (Constructor<OException>) iExceptionClass.getConstructor(String.class);
				exceptionToThrow = c.newInstance(msg);
			} catch (SecurityException e1) {
			} catch (NoSuchMethodException e1) {
			} catch (IllegalArgumentException e1) {
			} catch (InstantiationException e1) {
			} catch (IllegalAccessException e1) {
			} catch (InvocationTargetException e1) {
			}

		if (exceptionToThrow != null)
			throw exceptionToThrow;
		else
			throw new IllegalArgumentException("Cannot create the exception of type: " + iExceptionClass);
	}

	public boolean isWarn() {
		return warn;
	}

	public void setWarnEnabled(boolean warn) {
		this.warn = warn;
	}

	public void setInfoEnabled(boolean info) {
		this.info = info;
	}

	public void setDebugEnabled(boolean debug) {
		this.debug = debug;
	}

	public void setErrorEnabled(boolean error) {
		this.error = error;
	}

	public boolean isDebugEnabled() {
		return debug;
	}

	public boolean isInfoEnabled() {
		return info;
	}

	public boolean isWarnEnabled() {
		return warn;
	}

	public boolean isErrorEnabled() {
		return error;
	}

	public static OLogManager instance() {
		return instance;
	}

	public Level setLevel(final String iLevel, final Class<? extends Handler> iHandler) {
		final Level level = iLevel != null ? Level.parse(iLevel.toUpperCase(Locale.ENGLISH)) : Level.INFO;

		if (level.intValue() < minimumLevel.intValue()) {
			// UPDATE MINIMUM LEVEL
			minimumLevel = level;

			if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
				debug = info = warn = error = true;
			else if (level.equals(Level.INFO)) {
				info = warn = error = true;
				debug = false;
			} else if (level.equals(Level.WARNING)) {
				warn = error = true;
				debug = info = false;
			} else if (level.equals(Level.SEVERE)) {
				error = true;
				debug = info = warn = false;
			}
		}

		Logger log = Logger.getLogger("");
		for (Handler h : log.getHandlers()) {
			if (h.getClass().isAssignableFrom(iHandler)) {
				h.setLevel(level);
				break;
			}
		}

		return level;
	}

	public static void installCustomFormatter() {
		try {
			// ASSURE TO HAVE THE ORIENT LOG FORMATTER TO THE CONSOLE EVEN IF NO CONFIGURATION FILE IS TAKEN
			final Logger log = Logger.getLogger("");
			if (log.getHandlers().length == 0) {
				// SET DEFAULT LOG FORMATTER
				final Handler h = new ConsoleHandler();
				h.setFormatter(new OLogFormatter());
				log.addHandler(h);
			} else {
				for (Handler h : log.getHandlers()) {
					if (h instanceof ConsoleHandler && !h.getFormatter().getClass().equals(OLogFormatter.class))
						h.setFormatter(new OLogFormatter());
				}
			}
		} catch (Exception e) {
			System.err.println("Error while installing custom formatter. Logging could be disabled. Cause: " + e.toString());
		}
	}
}

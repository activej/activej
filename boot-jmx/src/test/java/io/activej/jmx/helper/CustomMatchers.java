package io.activej.jmx.helper;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public final class CustomMatchers {

	public static ObjectName objectName(String name) {
		try {
			return new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			throw new AssertionError(e);
		}
	}
}

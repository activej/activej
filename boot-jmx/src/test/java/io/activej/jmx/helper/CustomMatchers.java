package io.activej.jmx.helper;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public final class CustomMatchers {

	public static Matcher<ObjectName> objectname(String name) {
		return new BaseMatcher<>() {
			@Override
			public boolean matches(Object item) {
				try {
					return new ObjectName(name).equals(item);
				} catch (MalformedObjectNameException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("ObjectName: " + name);
			}
		};
	}
}

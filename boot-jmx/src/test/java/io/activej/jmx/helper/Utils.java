package io.activej.jmx.helper;

import javax.management.MBeanAttributeInfo;
import java.util.HashMap;
import java.util.Map;

public class Utils {
	public static Map<String, MBeanAttributeInfo> nameToAttribute(MBeanAttributeInfo[] attrs) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attr : attrs) {
			nameToAttr.put(attr.getName(), attr);
		}
		return nameToAttr;
	}
}

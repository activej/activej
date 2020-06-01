package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.helper.Utils;
import org.junit.Test;

import javax.management.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicMBeanFactoryAttributesBulkGettersTest {
	@Test
	public void bulkGetOmitsAttributesWithExceptionButReturnsValidAttributes() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(new MBeanStub()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> attrs = Utils.nameToAttribute(mbean.getMBeanInfo().getAttributes());

		String[] expectedAttrNames = {"text", "value", "number"};
		assertEquals(new HashSet<>(asList(expectedAttrNames)), attrs.keySet());

		AttributeList fetchedAttrs = mbean.getAttributes(expectedAttrNames);
		assertEquals(2, fetchedAttrs.size());

		Map<String, Object> attrNameToValue = nameToAttribute(fetchedAttrs);

		assertTrue(attrNameToValue.containsKey("text"));
		assertEquals("data", attrNameToValue.get("text"));

		assertTrue(attrNameToValue.containsKey("number"));
		assertEquals(100L, attrNameToValue.get("number"));
	}

	@Test(expected = MBeanException.class)
	public void propagatesExceptionInCaseOfSingleAttributeGet() throws Exception {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(new MBeanStub()), defaultSettings(), false);

		mbean.getAttribute("value");
	}

	public static final class MBeanStub implements ConcurrentJmxBean {

		@JmxAttribute
		public String getText() {
			return "data";
		}

		@JmxAttribute
		public int getValue() {
			throw new RuntimeException("custom");
		}

		@JmxAttribute
		public long getNumber() {
			return 100L;
		}
	}

	private static Map<String, Object> nameToAttribute(AttributeList fetchedAttrs) {
		Map<String, Object> nameToValue = new HashMap<>();
		for (Object fetchAttr : fetchedAttrs) {
			Attribute attr = (Attribute) fetchAttr;
			nameToValue.put(attr.getName(), attr.getValue());
		}
		return nameToValue;
	}
}

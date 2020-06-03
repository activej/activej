package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import java.util.Map;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static io.activej.jmx.helper.Utils.nameToAttribute;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;

public class DynamicMBeanFactoryAttributeDescriptionTest {

	@Test
	public void ifDescriptionIsNotSpecifiedItIsSameAsFullNameOfAttribute() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(new MBeanWithNoJmxDescription()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals("stats_count", nameToAttr.get("stats_count").getDescription());
	}

	public static final class MBeanWithNoJmxDescription implements ConcurrentJmxBean {
		@JmxAttribute
		public SimplePojo getStats() {
			return new SimplePojo();
		}

		public static final class SimplePojo {
			@JmxAttribute
			public int getCount() {
				return 0;
			}
		}
	}

	@Test
	public void showsDescriptionWithoutChangesIfAttributeNameDoNotContainUnderscores() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(new MBeanWithDescriptionInDirectNonPojoAttribute()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals("description of count", nameToAttr.get("count").getDescription());
		assertEquals("description of inner count", nameToAttr.get("innerCount").getDescription());
	}

	public static final class MBeanWithDescriptionInDirectNonPojoAttribute implements ConcurrentJmxBean {
		@JmxAttribute(description = "description of count")
		public int getCount() {
			return 0;
		}

		@JmxAttribute(name = "")
		public Group getGroup() {
			return new Group();
		}

		public static final class Group {
			@JmxAttribute(description = "description of inner count")
			public int getInnerCount() {
				return 0;
			}
		}
	}

	@Test
	public void formatsDescriptionsProperlyIfAttributeNameContainsUnderscores() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(new MBeanWithPojoDescription()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals("\"stats\": desc of first-level pojo  |  \"info\": desc of info",
				nameToAttr.get("stats_innerStats_info").getDescription());
	}

	public static final class MBeanWithPojoDescription implements ConcurrentJmxBean {
		@JmxAttribute(description = "desc of first-level pojo")
		public Stats getStats() {
			return new Stats();
		}

		public static final class Stats {
			@JmxAttribute // no description here
			public InnerStats getInnerStats() {
				return new InnerStats();
			}

			public static final class InnerStats {
				@JmxAttribute(description = "desc of info")
				public String getInfo() {
					return "";
				}
			}
		}
	}
}

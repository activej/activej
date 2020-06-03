package io.activej.jmx;

import io.activej.jmx.DynamicMBeanFactory.JmxCustomTypeAdapter;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.JmxStats;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.activej.jmx.helper.Utils.nameToAttribute;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicMBeanFactorySettingsTest {
	private static final Set<String> NO_BEANS = Collections.emptySet();
	private static final Map<String, AttributeModifier<?>> NO_MODIFIERS = Collections.emptyMap();
	private static final Map<Type, JmxCustomTypeAdapter<?>> NO_CUSTOM_TYPES = Collections.emptyMap();

	// region included optionals
	@Test
	public void includesOptionalAttributes_thatAreSpecifiedInSettings() {
		JmxBeanSettings settings = JmxBeanSettings.of(singleton("stats_text"), NO_MODIFIERS, NO_CUSTOM_TYPES);
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(new MBeanStubOne()), settings, false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mBeanInfo.getAttributes());

		assertEquals(2, attrs.size());
		assertTrue(attrs.containsKey("stats_text"));
	}

	public static final class MBeanStubOne implements ConcurrentJmxBean {
		private final JmxStatsStub stats = new JmxStatsStub();

		@JmxAttribute
		public JmxStatsStub getStats() {
			return stats;
		}
	}

	public static final class JmxStatsStub implements JmxStats<JmxStatsStub> {

		@Override
		public void add(JmxStatsStub another) {

		}

		@JmxAttribute
		public int getNumber() {
			return 10;
		}

		@JmxAttribute(optional = true)
		public String getText() {
			return "text";
		}

		@JmxAttribute(optional = true)
		public String getData() {
			return "data";
		}
	}
	// endregion

	// region modifiers
	@Test
	public void modifiesDynamicMBeanComponentsAccordingToSettings() throws Exception {
		Map<String, AttributeModifier<?>> nameToModifier = new HashMap<>();
		nameToModifier.put("stats", (AttributeModifier<ConfigurableStats>) attribute -> attribute.setConfigurableText("configured"));
		JmxBeanSettings settings = JmxBeanSettings.of(NO_BEANS, nameToModifier, NO_CUSTOM_TYPES);
		MBeanStubTwo mBeanStubTwo = new MBeanStubTwo();
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(singletonList(mBeanStubTwo), settings, false);

		assertEquals("configured", mbean.getAttribute("stats_data"));
	}

	public static final class MBeanStubTwo implements ConcurrentJmxBean {
		private final ConfigurableStats stats = new ConfigurableStats();

		@JmxAttribute
		public ConfigurableStats getStats() {
			return stats;
		}
	}

	public static final class ConfigurableStats implements JmxStats<ConfigurableStats> {
		private String configurableText = "";

		@JmxAttribute
		public String getData() {
			return configurableText;
		}

		public void setConfigurableText(String text) {
			configurableText = text;
		}

		@Override
		public void add(ConfigurableStats another) {
			this.configurableText += another.configurableText;
		}
	}

	// endregion
}

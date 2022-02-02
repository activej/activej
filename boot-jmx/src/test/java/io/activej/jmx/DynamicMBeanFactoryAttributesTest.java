package io.activej.jmx;

import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.jmx.api.JmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.helper.JmxBeanAdapterStub;
import io.activej.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.Attribute;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.*;

import static io.activej.jmx.JmxBeanSettings.defaultSettings;
import static io.activej.jmx.helper.Utils.nameToAttribute;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

public class DynamicMBeanFactoryAttributesTest {
	@Test
	public void retrievesProperMBeanInfo() {
		MBeanWithSimpleAttrsAndPojo mbeanOneSample = new MBeanWithSimpleAttrsAndPojo("data", new SamplePojo(5, 100));
		DynamicMBean mbean = createDynamicMBeanFor(mbeanOneSample);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(3, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("info"));
		assertTrue(nameToAttr.get("info").isReadable());

		assertTrue(nameToAttr.containsKey("details_count"));
		assertTrue(nameToAttr.get("details_count").isReadable());

		assertTrue(nameToAttr.containsKey("details_sum"));
		assertTrue(nameToAttr.get("details_sum").isReadable());
	}

	@Test
	public void retrievesProperAttributeValues() throws Exception {
		MBeanWithSimpleAttrsAndPojo mbeanOneSample = new MBeanWithSimpleAttrsAndPojo("data", new SamplePojo(5, 100L));
		DynamicMBean mbean = createDynamicMBeanFor(mbeanOneSample);

		assertEquals("data", mbean.getAttribute("info"));
		assertEquals(5, mbean.getAttribute("details_count"));
		assertEquals(100L, mbean.getAttribute("details_sum"));
	}

	/*
	 * tests for aggregation
	 */
	@Test
	public void ifAppropriateAttributesInDifferentMBeansHaveSameValueReturnsThatValue() throws Exception {
		MBeanWithSingleIntAttr mbean_1 = new MBeanWithSingleIntAttr(1);
		MBeanWithSingleIntAttr mbean_2 = new MBeanWithSingleIntAttr(1);

		DynamicMBean mbean = createDynamicMBeanFor(mbean_1, mbean_2);

		assertEquals(1, mbean.getAttribute("value"));
	}

	@Test
	public void ifAppropriateAttributesInDifferentMBeansHaveDifferentValueReturnsNull() throws Exception {
		MBeanWithSingleIntAttr mbean_1 = new MBeanWithSingleIntAttr(1);
		MBeanWithSingleIntAttr mbean_2 = new MBeanWithSingleIntAttr(2);

		DynamicMBean mbean = createDynamicMBeanFor(mbean_1, mbean_2);

		assertNull(mbean.getAttribute("value"));
	}

	@Test
	public void aggregatesJmxStatsUsingTheirAggregationPolicy() throws Exception {
		JmxStatsStub stats_1 = new JmxStatsStub();
		stats_1.recordValue(100);
		stats_1.recordValue(20);

		JmxStatsStub stats_2 = new JmxStatsStub();
		stats_2.recordValue(5);

		MBeanWithJmxStats mBeanWithJmxStats_1 = new MBeanWithJmxStats(stats_1);
		MBeanWithJmxStats mBeanWithJmxStats_2 = new MBeanWithJmxStats(stats_2);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithJmxStats_1, mBeanWithJmxStats_2);

		assertEquals(3, mbean.getAttribute("jmxStatsStub_count"));
		assertEquals(125L, mbean.getAttribute("jmxStatsStub_sum"));
	}

	@Test
	public void concatenatesListAttributesFromDifferentMBeans() throws Exception {
		MBeanWithListAttr mBeanWithListAttr_1 = new MBeanWithListAttr(List.of("a", "b"));
		MBeanWithListAttr mBeanWithListAttr_2 = new MBeanWithListAttr(new ArrayList<>());
		MBeanWithListAttr mBeanWithListAttr_3 = new MBeanWithListAttr(List.of("w"));

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithListAttr_1, mBeanWithListAttr_2, mBeanWithListAttr_3);

		assertArrayEquals(new String[]{"a", "b", "w"}, (Object[]) mbean.getAttribute("list"));
	}

	@Test
	public void properlyAggregateMapsByKeyAccordingToTheirValueAggregationPolicy() throws Exception {
		Map<String, Integer> map_1 = new HashMap<>();
		map_1.put("a", 1);
		map_1.put("b", 2);
		map_1.put("c", 100);

		Map<String, Integer> map_2 = new HashMap<>();
		map_2.put("b", 2);
		map_2.put("c", 200);
		map_2.put("d", 5);

		MBeanWithMap mBeanWithMap_1 = new MBeanWithMap(map_1);
		MBeanWithMap mBeanWithMap_2 = new MBeanWithMap(map_2);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithMap_1, mBeanWithMap_2);

		TabularData tabularData = (TabularData) mbean.getAttribute("nameToNumber");

		assertEquals(4, tabularData.size());

		CompositeData row_1 = tabularData.get(keyForTabularData("a"));
		assertEquals(1, row_1.get("value"));

		CompositeData row_2 = tabularData.get(keyForTabularData("b"));
		assertEquals(2, row_2.get("value"));

		CompositeData row_3 = tabularData.get(keyForTabularData("c"));
		assertNull(row_3.get("value"));

		CompositeData row_4 = tabularData.get(keyForTabularData("d"));
		assertEquals(5, row_4.get("value"));
	}

	// test empty names
	@Test
	public void handlesEmptyAttributeNamesProperly() throws Exception {
		MBeanWithEmptyNames mBeanWithEmptyNames =
				new MBeanWithEmptyNames(
						new SamplePojo_L_1_1(10),
						new SamplePojo_L_1_2(
								new SamplePojo_L_2(25)
						)
				);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithEmptyNames);

		MBeanAttributeInfo[] attributesInfoArr = mbean.getMBeanInfo().getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(2, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("group_count"));
		assertTrue(nameToAttr.containsKey("group_total"));

		assertEquals(10, mbean.getAttribute("group_count"));
		assertEquals(25, mbean.getAttribute("group_total"));
	}

	// test empty leaf names
	@Test
	public void handlesAttributeWithEmptyLeafNamesProperly() throws Exception {
		MBeanWithEmptyLeafName bean = new MBeanWithEmptyLeafName();
		DynamicMBean mbean = createDynamicMBeanFor(bean);

		MBeanAttributeInfo[] attributesInfoArr = mbean.getMBeanInfo().getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(1, nameToAttr.size());
		assertTrue(nameToAttr.containsKey("pojo"));
		assertEquals(10, mbean.getAttribute("pojo"));
	}

	// test setters
	@Test
	public void returnsInfoAboutWritableAttributesInMBeanInfo() {
		MBeanWithSettableAttributes settableMBean = new MBeanWithSettableAttributes(10, 20, "data");
		DynamicMBean mbean = createDynamicMBeanFor(settableMBean);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(3, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("notSettableInt"));
		assertFalse(nameToAttr.get("notSettableInt").isWritable());

		assertTrue(nameToAttr.containsKey("settableInt"));
		assertTrue(nameToAttr.get("settableInt").isWritable());

		assertTrue(nameToAttr.containsKey("settableStr"));
		assertTrue(nameToAttr.get("settableStr").isWritable());
	}

	@Test
	public void setsWritableAttributes() throws Exception {
		MBeanWithSettableAttributes settableMBean = new MBeanWithSettableAttributes(10, 20, "data");
		DynamicMBean mbean = createDynamicMBeanFor(settableMBean);

		mbean.setAttribute(new Attribute("settableInt", 125));
		mbean.setAttribute(new Attribute("settableStr", "data-2"));

		assertEquals(125, mbean.getAttribute("settableInt"));
		assertEquals("data-2", mbean.getAttribute("settableStr"));
	}

	@Test
	public void setsWritableAttributesInInnerPojo() throws Exception {
		PojoWithSettableAttribute pojo = new PojoWithSettableAttribute(20);
		MBeanWithPojoWithSettableAttribute settableMBean = new MBeanWithPojoWithSettableAttribute(pojo);
		DynamicMBean mbean = createDynamicMBeanFor(settableMBean);

		mbean.setAttribute(new Attribute("pojo_sum", 155L));

		assertEquals(155L, mbean.getAttribute("pojo_sum"));
	}

	@Test
	public void worksProperlyWithIsGetters() throws Exception {
		MBeanWithIsGetter mBeanWithIsGetter = new MBeanWithIsGetter();
		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithIsGetter);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();

		assertEquals(1, attributesInfoArr.length);

		MBeanAttributeInfo booleanAttr = attributesInfoArr[0];
		assertEquals("active", booleanAttr.getName());
		assertTrue(booleanAttr.isReadable());
		assertTrue(booleanAttr.isIs());

		assertTrue((boolean) mbean.getAttribute("active"));
	}

	@Test
	public void throwsErrorForUnsupportedTypesInJmxAttributes() {

		ArbitraryType arbitraryType = new ArbitraryType("String representation");
		Date date = new Date();
		MBeanWithJmxAttributesOfArbitraryTypes obj =
				new MBeanWithJmxAttributesOfArbitraryTypes(arbitraryType, date);
		try {
			createDynamicMBeanFor(obj);
			fail();
		} catch (Exception ignored) {
		}
	}

	@Test
	public void handlesProperlyDefaultGetter() throws Exception {
		DynamicMBean mbean = createDynamicMBeanFor(new MBeanWithPojoWithDefaultGetter());

		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals(1, attrs.size());
		assertTrue(attrs.containsKey("pojo"));
		assertEquals("summary", mbean.getAttribute("pojo"));
	}

	@Test
	public void itShouldThrowExceptionForNonPublicAttributes() {
		MBeanWithNonPublicAttributes instance = new MBeanWithNonPublicAttributes();
		DynamicMBeanFactory dynamicMBeanFactory = DynamicMBeanFactory.create();
		List<MBeanWithNonPublicAttributes> beans = List.of(instance);
		JmxBeanSettings settings = defaultSettings();
		try {
			dynamicMBeanFactory.createDynamicMBean(beans, settings, false);
			fail();
		} catch (IllegalStateException e) {
			assertThat(e.getMessage(), containsString( "A method \"getValue\" in class '" + MBeanWithNonPublicAttributes.class.getName() +
					"' annotated with @JmxAttribute should be declared public"));
		}
	}

	// region helper methods
	public static DynamicMBean createDynamicMBeanFor(Object... objects) {
		return DynamicMBeanFactory.create()
				.createDynamicMBean(List.of(objects), defaultSettings(), false);
	}

	public static Object[] keyForTabularData(String key) {
		return new String[]{key};
	}
	// endregion

	// region helper classes
	public static final class SamplePojo {
		private final int count;
		private final long sum;

		public SamplePojo(int count, long sum) {
			this.count = count;
			this.sum = sum;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}

		@JmxAttribute
		public long getSum() {
			return sum;
		}
	}

	public static final class MBeanWithSimpleAttrsAndPojo implements ConcurrentJmxBean {
		private final String info;
		private final SamplePojo samplePojo;

		public MBeanWithSimpleAttrsAndPojo(String info, SamplePojo samplePojo) {
			this.info = info;
			this.samplePojo = samplePojo;
		}

		@JmxAttribute
		public String getInfo() {
			return info;
		}

		@JmxAttribute(name = "details")
		public SamplePojo getSamplePojo() {
			return samplePojo;
		}
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanWithListAttr {
		private final List<String> list;

		public MBeanWithListAttr(List<String> list) {
			this.list = list;
		}

		@JmxAttribute
		public List<String> getList() {
			return list;
		}
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanWithSingleIntAttr {
		private final int value;

		public MBeanWithSingleIntAttr(int value) {
			this.value = value;
		}

		@JmxAttribute
		public int getValue() {
			return value;
		}
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanWithJmxStats {
		private final JmxStatsStub jmxStatsStub;

		public MBeanWithJmxStats(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStatsStub() {
			return jmxStatsStub;
		}
	}

	@JmxBean(JmxBeanAdapterStub.class)
	public static final class MBeanWithMap {
		private final Map<String, Integer> nameToNumber;

		public MBeanWithMap(Map<String, Integer> nameToNumber) {
			this.nameToNumber = nameToNumber;
		}

		@JmxAttribute
		public Map<String, Integer> getNameToNumber() {
			return nameToNumber;
		}
	}

	public static class MBeanWithNonPublicAttributes implements ConcurrentJmxBean {
		private int value;

		@JmxAttribute
		public void setValue(int value) {
			this.value = value;
		}

		@JmxAttribute
		int getValue() {
			return value;
		}
	}

	// classes for empty names tests
	public static final class MBeanWithEmptyNames implements ConcurrentJmxBean {
		private final SamplePojo_L_1_1 pojo1;
		private final SamplePojo_L_1_2 pojo2;

		public MBeanWithEmptyNames(SamplePojo_L_1_1 pojo1, SamplePojo_L_1_2 pojo2) {
			this.pojo1 = pojo1;
			this.pojo2 = pojo2;
		}

		@JmxAttribute(name = "group")
		public SamplePojo_L_1_1 getPojo1() {
			return pojo1;
		}

		@JmxAttribute(name = "")
		public SamplePojo_L_1_2 getPojo2() {
			return pojo2;
		}
	}

	public static final class SamplePojo_L_1_1 {
		private final int count;

		public SamplePojo_L_1_1(int count) {
			this.count = count;
		}

		@JmxAttribute(name = "count")
		public int getCount() {
			return count;
		}
	}

	public static final class SamplePojo_L_1_2 {
		private final SamplePojo_L_2 pojo;

		public SamplePojo_L_1_2(SamplePojo_L_2 pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute(name = "group")
		public SamplePojo_L_2 getPojo() {
			return pojo;
		}
	}

	public static final class SamplePojo_L_2 {
		private final int total;

		public SamplePojo_L_2(int total) {
			this.total = total;
		}

		@JmxAttribute(name = "total")
		public int getTotal() {
			return total;
		}
	}

	// classes for empty leaf names
	public static final class MBeanWithEmptyLeafName implements ConcurrentJmxBean {
		private final PojoWithEmptyLeafName pojo = new PojoWithEmptyLeafName();

		@JmxAttribute
		public PojoWithEmptyLeafName getPojo() {
			return pojo;
		}
	}

	public static final class PojoWithEmptyLeafName {
		@JmxAttribute(name = "")
		public int getNumber() {
			return 10;
		}
	}

	// classes for setter tests
	public static final class MBeanWithSettableAttributes implements ConcurrentJmxBean {
		private final int notSettableInt;
		private int settableInt;
		private String settableStr;

		public MBeanWithSettableAttributes(int notSettableInt, int settableInt, String settableStr) {
			this.notSettableInt = notSettableInt;
			this.settableInt = settableInt;
			this.settableStr = settableStr;
		}

		@JmxAttribute
		public int getNotSettableInt() {
			return notSettableInt;
		}

		@JmxAttribute
		public int getSettableInt() {
			return settableInt;
		}

		@JmxAttribute
		public String getSettableStr() {
			return settableStr;
		}

		@JmxAttribute
		public void setSettableInt(int settableInt) {
			this.settableInt = settableInt;
		}

		@JmxAttribute
		public void setSettableStr(String settableStr) {
			this.settableStr = settableStr;
		}
	}

	public static final class MBeanWithPojoWithSettableAttribute implements ConcurrentJmxBean {
		private final PojoWithSettableAttribute pojo;

		public MBeanWithPojoWithSettableAttribute(PojoWithSettableAttribute pojo) {
			this.pojo = pojo;
		}

		@JmxAttribute
		public PojoWithSettableAttribute getPojo() {
			return pojo;
		}
	}

	public static final class PojoWithSettableAttribute {
		private long sum;

		public PojoWithSettableAttribute(long sum) {
			this.sum = sum;
		}

		@JmxAttribute
		public long getSum() {
			return sum;
		}

		@JmxAttribute
		public void setSum(long sum) {
			this.sum = sum;
		}
	}

	public static final class MBeanWithIsGetter implements ConcurrentJmxBean {

		@JmxAttribute
		public boolean isActive() {
			return true;
		}
	}

	public static final class MBeanWithJmxAttributesOfArbitraryTypes implements ConcurrentJmxBean {
		private final ArbitraryType arbitraryType;
		private final Date date;

		public MBeanWithJmxAttributesOfArbitraryTypes(ArbitraryType arbitraryType, Date date) {
			this.arbitraryType = arbitraryType;
			this.date = date;
		}

		@JmxAttribute
		public ArbitraryType getArbitraryType() {
			return arbitraryType;
		}

		@JmxAttribute
		public Date getDate() {
			return date;
		}
	}

	public static final class ArbitraryType {
		private final String str;

		public ArbitraryType(String str) {
			this.str = str;
		}

		@Override
		public String toString() {
			return str;
		}
	}

	// classes for default attribute test (@JmxAttribute get())
	public static final class MBeanWithPojoWithDefaultGetter implements ConcurrentJmxBean {
		private final PojoWithDefaultGetter pojo = new PojoWithDefaultGetter();

		@JmxAttribute
		public PojoWithDefaultGetter getPojo() {
			return pojo;
		}
	}

	public static final class PojoWithDefaultGetter {

		@JmxAttribute(optional = true)
		public int getValue() {
			return 10;
		}

		@JmxAttribute
		public String get() {
			return "summary";
		}
	}
	// endregion
}

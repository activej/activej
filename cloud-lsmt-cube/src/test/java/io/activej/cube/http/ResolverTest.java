package io.activej.cube.http;

public class ResolverTest {
	public static class Record {
		public int id;
		public String name1;
		public String name2;

		public Record(int id) {
			this.id = id;
		}
	}

/*
	public static class TestConstantResolver implements AttributeResolver {
		@Override
		public Map<PrimaryKey, Object[]> resolve(Set<PrimaryKey> keys, List<String> attributes) {
			Map<PrimaryKey, Object[]> result = newHashMap();
			for (PrimaryKey key : keys) {
				String name1 = key.get(0).toString() + key.get(1).toString();
				String name2 = "~" + name1;
				result.put(key, new Object[]{name1, name2});
			}
			return result;
		}
	}
*/

/*
	@Test
	public void testResolve() throws Exception {
		List<Object> records = Arrays.asList((Object) new Record(1), new Record(2), new Record(3));
		TestConstantResolver testAttributeResolver = new TestConstantResolver();

		Map<String, AttributeResolver> attributeResolvers = newLinkedHashMap();
		attributeResolvers.put("name1", testAttributeResolver);
		attributeResolvers.put("name2", testAttributeResolver);

		Map<AttributeResolver, List<String>> resolverKeys = newHashMap();
		resolverKeys.put(testAttributeResolver, Arrays.asList("id", "constantId"));

		Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		attributeTypes.put("name1", String.class);
		attributeTypes.put("name2", String.class);

		Map<String, Object> keyConstants = newHashMap();
		keyConstants.put("constantId", "ab");

		Resolver resolver = Resolver.create(attributeResolvers);

		List<Object> resultRecords = resolver.resolve(records, Record.class, attributeTypes, resolverKeys, keyConstants,
				DefiningClassLoader.create());

		assertEquals("1ab", ((Record) resultRecords.get(0)).name1);
		assertEquals("2ab", ((Record) resultRecords.get(1)).name1);
		assertEquals("3ab", ((Record) resultRecords.get(2)).name1);
		assertEquals("~1ab", ((Record) resultRecords.get(0)).name2);
		assertEquals("~2ab", ((Record) resultRecords.get(1)).name2);
		assertEquals("~3ab", ((Record) resultRecords.get(2)).name2);
		assertEquals(1, ((Record) resultRecords.get(0)).id);
		assertEquals(2, ((Record) resultRecords.get(1)).id);
		assertEquals(3, ((Record) resultRecords.get(2)).id);
	}
*/
}

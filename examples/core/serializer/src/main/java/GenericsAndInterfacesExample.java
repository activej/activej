import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.List;

public final class GenericsAndInterfacesExample {
	public static void main(String[] args) {
		//[START REGION_4]
		Developer developer = new Developer();
		developer.setSkills(List.of(
				new Skill<>(1, "Java"),
				new Skill<>(2, "ActiveJ")));

		byte[] buffer = new byte[200];
		BinarySerializer<Developer> serializer = SerializerBuilder.create()
				.build(Developer.class);
		//[END REGION_4]

		//[START REGION_5]
		serializer.encode(buffer, 0, developer);
		Developer developer2 = serializer.decode(buffer, 0);
		//[END REGION_5]

		//[START REGION_6]
		for (int i = 0; i < developer.getSkills().size(); i++) {
			System.out.println(developer.getSkills().get(i).getKey() + " - " + developer.getSkills().get(i).getValue() +
					", " + developer2.getSkills().get(i).getKey() + " - " + developer2.getSkills().get(i).getValue());
		}
		//[END REGION_6]
	}

	//[START REGION_2]
	public interface Person<K, V> {
		@Serialize
		List<Skill<K, V>> getSkills();
	}
	//[END REGION_2]

	//[START REGION_3]
	public static class Developer implements Person<Integer, String> {
		private List<Skill<Integer, String>> list;

		@Serialize
		@Override
		public List<Skill<Integer, String>> getSkills() {
			return list;
		}

		public void setSkills(List<Skill<Integer, String>> list) {
			this.list = list;
		}
	}
	//[END REGION_3]

	//[START REGION_1]
	public static class Skill<K, V> {
		private final K key;
		private final V value;

		public Skill(@Deserialize("key") K key,
				@Deserialize("value") V value) {
			this.key = key;
			this.value = value;
		}

		@Serialize
		public K getKey() {
			return key;
		}

		@Serialize
		public V getValue() {
			return value;
		}
	}
	//[END REGION_1]
}

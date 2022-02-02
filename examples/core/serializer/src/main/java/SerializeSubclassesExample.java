import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;

import java.util.ArrayList;
import java.util.List;

public final class SerializeSubclassesExample {

	//[START HOLDER]
	public static class ListHolder {
		@Serialize
		public List<@SerializeClass(subclassesId = "list") Object> list = new ArrayList<>();
	}
	//[END HOLDER]

	//[START MAIN]
	public static void main(String[] arg) {
		List<Object> list1 = List.of(123, 456L, "text", false, "content", true);
		serializeList(list1, Integer.class, Long.class, String.class, Boolean.class);

		System.out.println();

		List<Object> list2 = List.of((byte) 1, 12.34, 4353.323f, 'X');
		serializeList(list2, Byte.class, Double.class, Float.class, String.class, Character.class);
	}
	//[END MAIN]

	//[START SERIALIZE]
	private static void serializeList(List<Object> list, Class<?>... subClasses) {
		ListHolder holder = new ListHolder();

		holder.list.addAll(list);

		BinarySerializer<ListHolder> serializer = SerializerBuilder.create()
				.withSubclasses("list", List.of(subClasses))
				.build(ListHolder.class);

		byte[] buffer = new byte[1024];
		serializer.encode(buffer, 0, holder);
		ListHolder decoded = serializer.decode(buffer, 0);

		printLists(list, decoded.list);
	}
	//[END SERIALIZE]

	private static final String FORMAT_PATTERN = "%-10s%-10s%n";

	private static void printLists(List<Object> list, List<Object> decodedList) {
		assert list.size() == decodedList.size();

		System.out.printf(FORMAT_PATTERN, "ORIGINAL", "DECODED");
		System.out.printf(FORMAT_PATTERN, "--------", "--------");
		for (int i = 0; i < list.size(); i++) {
			Object element = list.get(i);
			Object decodedElement = decodedList.get(i);
			System.out.printf(FORMAT_PATTERN, element, decodedElement);
		}
	}
}

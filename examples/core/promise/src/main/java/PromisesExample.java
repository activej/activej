import io.activej.promise.Promise;
import io.activej.promise.Promises;

import java.util.Arrays;

public final class PromisesExample {
	private static int counter;

	private static void repeat() {
		System.out.println("Repeat until exception:");
		//[START REGION_1]
		Promises.repeat(() -> {
			System.out.println("This is iteration #" + ++counter);
			if (counter == 5) {
				return Promise.of(false);
			}
			return Promise.of(true);
		});
		//[END REGION_1]
		System.out.println();
	}

	private static void loop() {
		System.out.println("Looping with condition:");
		//[START REGION_2]
		Promises.loop(0,
				i -> i < 5,
				i -> {
					System.out.println("This is iteration #" + i);
					return Promise.of(i + 1);
				});
		//[END REGION_2]
		System.out.println();
	}

	private static void toList() {
		System.out.println("Collecting group of Promises to list of Promises' results:");
		//[START REGION_3]
		Promises.toList(Promise.of(1), Promise.of(2), Promise.of(3), Promise.of(4), Promise.of(5), Promise.of(6))
				.whenResult(list -> System.out.println("Size of collected list: " + list.size() + "\nList: " + list));
		//[END REGION_3]
		System.out.println();
	}

	private static void toArray() {
		System.out.println("Collecting group of Promises to array of Promises' results:");
		//[START REGION_4]
		Promises.toArray(Integer.class, Promise.of(1), Promise.of(2), Promise.of(3), Promise.of(4), Promise.of(5), Promise.of(6))
				.whenResult(array -> System.out.println("Size of collected array: " + array.length + "\nArray: " + Arrays.toString(array)));
		//[END REGION_4]
		System.out.println();
	}

	public static void main(String[] args) {
		repeat();
		loop();
		toList();
		toArray();
	}
}

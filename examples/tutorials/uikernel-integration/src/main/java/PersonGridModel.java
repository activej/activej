import io.activej.inject.annotation.Inject;
import io.activej.promise.Promise;
import io.activej.uikernel.*;

import java.util.*;
import java.util.function.Predicate;

import static io.activej.common.Checks.checkNotNull;
import static java.util.stream.Collectors.toList;

@Inject
public class PersonGridModel implements GridModel<Integer, Person> {
	private final Map<String, Comparator<Person>> comparators = createComparators();
	private final Map<Integer, Person> storage = initStorage();
	private int cursor = storage.size() + 1;

	private static Map<Integer, Person> initStorage() {
		Map<Integer, Person> storage = new HashMap<>();
		storage.put(1, new Person(1, "Humphrey", "555-0186", 77, Gender.FEMALE));
		storage.put(2, new Person(2, "Thornton", "555-0108", 73, Gender.MALE));
		storage.put(3, new Person(3, "Addie", "555-0105", 33, Gender.MALE));
		storage.put(4, new Person(4, "Kelsey", "555-0189", 44, Gender.FEMALE));
		storage.put(5, new Person(5, "Sonya", "555-0153", 33, Gender.FEMALE));
		storage.put(6, new Person(6, "Adams", "555-0175", 88, Gender.MALE));
		storage.put(7, new Person(7, "Rodriguez", "555-0133", 55, Gender.MALE));
		storage.put(8, new Person(8, "Acosta", "555-0100", 22, Gender.MALE));
		storage.put(9, new Person(9, "Murray", "555-0132", 99, Gender.MALE));
		storage.put(10, new Person(10, "Francine", "555-0153", 63, Gender.FEMALE));
		storage.put(11, new Person(11, "Angelica", "555-0125", 19, Gender.FEMALE));
		storage.put(12, new Person(12, "Kaya", "555-0179", 34, Gender.FEMALE));
		storage.put(13, new Person(13, "Roach", "555-0118", 55, Gender.MALE));
		storage.put(14, new Person(14, "Evangeline", "555-0153", 18, Gender.FEMALE));
		return storage;
	}

	private static Map<String, Comparator<Person>> createComparators() {
		Map<String, Comparator<Person>> comparators = new HashMap<>();
		comparators.put("name", Comparator.comparing(Person::getName));
		comparators.put("phone", Comparator.comparing(Person::getPhone));
		comparators.put("age", Comparator.comparingInt(Person::getAge));
		comparators.put("gender", Comparator.comparing(Person::getGender));
		return comparators;
	}

	@Override
	public Promise<Person> read(final Integer id, final ReadSettings<Integer> readSettings) {
		return Promise.of(storage.get(id));
	}

	@Override
	public Promise<ReadResponse<Integer, Person>> read(final ReadSettings<Integer> readSettings) {
		Predicate<Person> predicate = new PersonMatcher(readSettings.getFilters());
		List<Person> people = storage.values().stream().filter(predicate).collect(toList());
		for (Map.Entry<String, ReadSettings.SortOrder> entry : readSettings.getSort().entrySet()) {
			Comparator<Person> comparator = comparators.get(entry.getKey());
			if (entry.getValue() == ReadSettings.SortOrder.DESCENDING) {
				comparator = Collections.reverseOrder(comparator);
			}
			people.sort(comparator);
		}
		int count = people.size();

		int offset = readSettings.getOffset();
		if (offset <= people.size()) {
			people = people.subList(offset, Math.min(people.size(), offset + readSettings.getLimit()));
		} else {
			people = List.of();
		}
		return Promise.of(ReadResponse.of(people, count, List.of()));
	}

	@Override
	public Promise<CreateResponse<Integer>> create(final Person person) {
		person.setId(cursor);
		storage.put(cursor, person);
		cursor++;
		return Promise.of(CreateResponse.of(cursor));
	}

	@Override
	public Promise<UpdateResponse<Integer, Person>> update(final List<Person> changes) {
		List<Person> results = new ArrayList<>();
		for (Person change : changes) {
			Person person = storage.get(change.getId());
			checkNotNull(person);
			merge(person, change);
			results.add(person);
		}
		return Promise.of(UpdateResponse.of(results));
	}

	@Override
	public Promise<DeleteResponse> delete(final Integer id) {
		storage.remove(id);
		return Promise.of(DeleteResponse.ok());
	}

	@Override
	public Class<Integer> getIdType() {
		return Integer.class;
	}

	@Override
	public Class<Person> getRecordType() {
		return Person.class;
	}

	private void merge(Person person, Person change) {
		if (change.getName() != null) {
			person.setName(change.getName());
		}
		if (change.getPhone() != null) {
			person.setPhone(change.getPhone());
		}
		if (change.getAge() != null) {
			person.setAge(change.getAge());
		}
		if (change.getGender() != null) {
			person.setGender(change.getGender());
		}
	}

	private static class PersonMatcher implements Predicate<Person> {
		private final Map<String, String> filters;

		PersonMatcher(Map<String, String> filters) {
			this.filters = filters;
		}

		@Override
		public boolean test(Person person) {
			String searchParam = filters.get("search");
			if (searchParam != null && !searchParam.isEmpty() && !person.getName().contains(searchParam)) {
				return false;
			}

			String ageParam = filters.get("age");
			if (ageParam != null && !ageParam.isEmpty()) {
				int age = Integer.parseInt(ageParam);
				if (person.getAge() != age) {
					return false;
				}
			}

			String genderParam = filters.get("gender");
			if (genderParam != null && !genderParam.isEmpty() && (genderParam.equals("MALE") || genderParam.equals("FEMALE"))) {
				Gender gender = Gender.valueOf(genderParam);
				return person.getGender() == gender;
			}

			return true;
		}
	}
}

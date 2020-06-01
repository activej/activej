package util;

import java.time.LocalDate;

public final class Person {
	private final int id;
	private final String name;
	private final LocalDate dateOfBirth;

	public Person(int id, String name, LocalDate dateOfBirth) {
		this.name = name;
		this.id = id;
		this.dateOfBirth = dateOfBirth;
	}

	public int getId() {
		return id;
	}

	String getName() {
		return name;
	}

	LocalDate getDateOfBirth() {
		return dateOfBirth;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Person person = (Person) o;

		if (id != person.id) return false;
		if (!name.equals(person.name)) return false;
		return dateOfBirth.equals(person.dateOfBirth);
	}

	@Override
	public int hashCode() {
		int result = id;
		result = 31 * result + name.hashCode();
		result = 31 * result + dateOfBirth.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "Person{" +
				"id=" + id +
				", name='" + name + '\'' +
				", dateOfBirth=" + dateOfBirth +
				'}';
	}
}

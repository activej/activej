package userservice.dao;

import java.util.Objects;

/**
 * A simple DTO (Data Transfer Object) class that encapsulate user's first and last names
 */
public final class User {
	private final String firstName;
	private final String lastName;

	public User(String firstName, String lastName) {
		this.firstName = firstName;
		this.lastName = lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		User user = (User) o;
		return firstName.equals(user.firstName) && lastName.equals(user.lastName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(firstName, lastName);
	}

	@Override
	public String toString() {
		return "User{" +
				"firstName='" + firstName + '\'' +
				", lastName='" + lastName + '\'' +
				'}';
	}
}

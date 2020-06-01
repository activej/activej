class Contact {
	private final String name;
	private final Integer age;
	private final Address address;

	public Contact(String name, Integer age, Address address) {
		this.name = name;
		this.age = age;
		this.address = address;
	}

	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public Address getAddress() {
		return address;
	}

	@Override
	public String toString() {
		return "Contact{name='" + name + '\'' + ", age=" + age + ", address=" + address + '}';
	}
}

class Address {
	private final String title;

	public Address(String title) {
		this.title = title;
	}

	public String getTitle() {
		return title;
	}

	@Override
	public String toString() {
		return "Address{title='" + title + '\'' + '}';
	}
}

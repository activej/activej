import io.activej.uikernel.AbstractRecord;

public class Person extends AbstractRecord<Integer> {
	private String name;
	private String phone;
	private Integer age;
	private Gender gender;

	public Person() {
	}

	public Person(int id, String name, String phone, Integer age, Gender gender) {
		this.id = id;
		this.name = name;
		this.phone = phone;
		this.age = age;
		this.gender = gender;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public Gender getGender() {
		return gender;
	}

	public void setGender(Gender gender) {
		this.gender = gender;
	}

	@Override
	public String toString() {
		return "Person{" +
			   "id=" + getId() +
			   ", name='" + name + '\'' +
			   ", phone='" + phone + '\'' +
			   ", age=" + age +
			   ", gender=" + gender +
			   '}';
	}
}

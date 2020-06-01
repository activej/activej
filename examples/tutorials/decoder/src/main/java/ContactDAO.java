import java.util.List;

interface ContactDAO {
	List<Contact> list();

	void add(Contact user);
}

import java.util.ArrayList;
import java.util.List;

class ContactDAOImpl implements ContactDAO {
	private List<Contact> userList = new ArrayList<>();

	@Override
	public List<Contact> list() {
		return userList;
	}

	@Override
	public void add(Contact user) {
		userList.add(user);
	}
}

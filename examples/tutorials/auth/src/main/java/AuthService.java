//[START EXAMPLE]
public interface AuthService {

	boolean authorize(String login, String password);

	void register(String login, String password);
}
//[END EXAMPLE]

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

//[START EXAMPLE]
public interface PollDao {
	Poll find(int id);
	Map<Integer, Poll> findAll();
	int add(Poll poll);
	void update(int id, Poll poll);
	void remove(int id);

	class Poll {
		private final String title;
		private final String message;
		private final Map<String, Integer> optionsToVote;

		public Poll(String title, String message, List<String> options) {
			this.title = title;
			this.message = message;
			this.optionsToVote = new LinkedHashMap<>();

			for (String option : options) {
				optionsToVote.put(option, 0);
			}
		}

		public void vote(String option) {
			Integer votesCount = optionsToVote.get(option);
			optionsToVote.put(option, ++votesCount);
		}

		public String getTitle() {
			return title;
		}

		public String getMessage() {
			return message;
		}

		public Set<Map.Entry<String, Integer>> getOptions() {
			return optionsToVote.entrySet();
		}

	}
}
//[END EXAMPLE]

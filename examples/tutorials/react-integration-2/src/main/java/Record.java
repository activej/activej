import java.util.List;

//[START EXAMPLE]
public final class Record {
	private String title;
	private final List<Plan> plans;

	public Record(String title, List<Plan> plans) {
		this.title = title;
		this.plans = plans;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<Plan> getPlans() {
		return plans;
	}
}
//[END EXAMPLE]

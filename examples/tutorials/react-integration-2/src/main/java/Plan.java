//[START EXAMPLE]
public final class Plan {
	private String text;
	private boolean isComplete;

	public Plan(String text, boolean isComplete) {
		this.text = text;
		this.isComplete = isComplete;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public boolean isComplete() {
		return isComplete;
	}

	public void toggle() {
		isComplete = !isComplete;
	}
}
//[END EXAMPLE]

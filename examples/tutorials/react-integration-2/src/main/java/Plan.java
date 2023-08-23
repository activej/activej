//[START EXAMPLE]
public final class Plan {
	private String text;
	private boolean complete;

	public Plan(String text, boolean complete) {
		this.text = text;
		this.complete = complete;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public boolean isComplete() {
		return complete;
	}

	public void toggle() {
		complete = !complete;
	}
}
//[END EXAMPLE]

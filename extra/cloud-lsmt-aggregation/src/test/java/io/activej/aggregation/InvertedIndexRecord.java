package io.activej.aggregation;

import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;

import java.util.Objects;

/**
 * First we define the structure of the input record which represents the word and id of the document that contains this word.
 */
public class InvertedIndexRecord {
	private final String word;
	private final int documentId;

	@Key("word")
	public String getWord() {
		return word;
	}

	@Measures("documents")
	public Integer getDocumentId() {
		return documentId;
	}

	public InvertedIndexRecord(String word, int documentId) {
		this.word = word;
		this.documentId = documentId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		InvertedIndexRecord that = (InvertedIndexRecord) o;

		if (!Objects.equals(word, that.word)) return false;
		return documentId == that.documentId;

	}

	@Override
	public int hashCode() {
		int result = word != null ? word.hashCode() : 0;
		result = 31 * result + documentId;
		return result;
	}

	@Override
	public String toString() {
		return "InvertedIndexRecord{" +
			"word='" + word + '\'' +
			", documentId=" + documentId +
			'}';
	}
}


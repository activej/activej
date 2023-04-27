package io.activej.reactor.schedule;

import io.activej.common.Utils;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

public final class ScheduledPriorityQueue {
	private ScheduledRunnable[] entries = new ScheduledRunnable[10];
	private int size;

	public int size() {
		return size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	public void add(ScheduledRunnable entry) {
		checkArgument(entry.queue == null);
		entry.queue = this;
		if (size >= entries.length) {
			entries = Arrays.copyOf(entries, entries.length * 3 / 2);
		}
		siftUp(entries, entry, size++);
	}

	@Nullable
	public ScheduledRunnable peek() {
		return entries[0];
	}

	@Nullable
	public ScheduledRunnable take(long now) {
		ScheduledRunnable entry = entries[0];
		if (entry == null || entry.timestamp > now) return null;
		size--;
		ScheduledRunnable entryLast = entries[size];
		entries[size] = null;
		if (size > 0) {
			siftDown(entries, size, entryLast, 0);
		}
		entry.queue = null;
		return entry;
	}

	void remove(ScheduledRunnable entry) {
		int index = entry.index;
		entry.queue = null;
		size--;
		if (size == index) {
			entries[index] = null;
		} else {
			ScheduledRunnable entryLast = entries[size];
			entries[size] = null;
			siftDown(entries, size, entryLast, index);
			if (entries[index] == entryLast) {
				siftUp(entries, entryLast, index);
			}
		}
	}

	private static void siftUp(ScheduledRunnable[] entries, ScheduledRunnable entry, int index) {
		while (index > 0) {
			int indexParent = (index - 1) >>> 1;
			ScheduledRunnable entryParent = entries[indexParent];
			if (entry.timestamp >= entryParent.timestamp) break;
			set(entries, index, entryParent);
			index = indexParent;
		}
		set(entries, index, entry);
	}

	private static void siftDown(ScheduledRunnable[] entries, int size, ScheduledRunnable entry, int index) {
		int half = size >>> 1;
		while (index < half) {
			int indexL = index * 2 + 1;
			int indexR = indexL + 1;
			ScheduledRunnable entryL = entries[indexL];
			ScheduledRunnable entryR = indexR < size ? entries[indexR] : null;
			int indexChild;
			ScheduledRunnable entryChild;
			if (entryR == null || entryL.timestamp <= entryR.timestamp) {
				indexChild = indexL;
				entryChild = entries[indexL];
			} else {
				indexChild = indexR;
				entryChild = entries[indexR];
			}
			if (entry.timestamp <= entryChild.timestamp) break;
			set(entries, index, entryChild);
			index = indexChild;
		}
		set(entries, index, entry);
	}

	private static void set(ScheduledRunnable[] entries, int index, ScheduledRunnable entry) {
		entries[index] = entry;
		entry.index = index;
	}

	@Override
	public String toString() {
		return Utils.toString(Arrays.stream(entries).limit(size).collect(toList()), 1 + 2 + 4);
	}
}

package insomnia.demo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import insomnia.lib.cpu.CPUTimeBenchmark;

public final class Measures
{
	private Map<String, Map<String, Object>> groups = new HashMap<>();

	// ==========================================================================

	public CPUTimeBenchmark getTime(String group, String measure)
	{
		return (CPUTimeBenchmark) groups.computeIfAbsent(group, s -> new HashMap<>()).computeIfAbsent(measure, k -> new CPUTimeBenchmark());
	}

	public int getInt(String group, String measure)
	{
		return (Integer) groups.computeIfAbsent(group, s -> new HashMap<>()).computeIfAbsent(measure, k -> 0);
	}

	public int[] getIntTab(String group, String measure)
	{
		return (int[]) groups.computeIfAbsent(group, s -> new HashMap<>()).computeIfAbsent(measure, k -> new int[] { 0 });
	}
	// ==========================================================================

	private void set_(String group, String measure, Object val)
	{
		groups.computeIfAbsent(group, s -> new HashMap<>()).put(measure, val);
	}

	public void set(String group, String measure, CPUTimeBenchmark val)
	{
		set_(group, measure, val);
	}

	public void set(String group, String measureName, String set)
	{
		set_(group, measureName, set);
	}

	public void set(String group, String measure, int val)
	{
		set_(group, measure, val);
	}

	public void set(String group, String measure, int val[])
	{
		set_(group, measure, val);
	}

	// ==========================================================================

	public void print(PrintStream print)
	{
		var sortedGroups = new ArrayList<>(groups.entrySet());
		Collections.sort(sortedGroups, Map.Entry.<String, Map<String, Object>>comparingByKey());

		for (var group : sortedGroups)
		{
			print.printf("\n[%s]\n", group.getKey());

			var sortedMeasures = new ArrayList<>(group.getValue().entrySet());
			Collections.sort(sortedMeasures, Map.Entry.<String, Object>comparingByKey());

			for (var measure : sortedMeasures)
				print.printf("%s: %s\n", measure.getKey(), measure.getValue());
		}
	}
}

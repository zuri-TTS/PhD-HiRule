package insomnia.demo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import insomnia.lib.cpu.CPUTimeBenchmark;

public final class Measures
{
	private Map<String, Map<String, Object>> groups = new HashMap<>();

	private String prefix = "";

	private String defaultGroup = "measures";

	public void setPrefix(String prefix)
	{
		this.prefix = prefix;
	}

	// ==========================================================================

	public void addAll(Measures toAdd)
	{
		groups.putAll(toAdd.get());
	}

	// ==========================================================================

	public Map<String, Map<String, Object>> get()
	{

		if (prefix.isEmpty())
			return Collections.unmodifiableMap(groups);

		return groups.entrySet().stream().collect(Collectors.toMap(e -> prefix + e.getKey(), Map.Entry::getValue));
	}

	public CPUTimeBenchmark getTime(String measure)
	{
		return getTime(defaultGroup, measure);
	}

	public long getLong(String measure)
	{
		return getLong(defaultGroup, measure);
	}

	public int getInt(String measure)
	{
		return getInt(defaultGroup, measure);
	}

	public int[] getIntTab(String measure)
	{
		return getIntTab(defaultGroup, measure);
	}

	public CPUTimeBenchmark getTime(String group, String measure)
	{
		return (CPUTimeBenchmark) groups.computeIfAbsent(group, s -> new HashMap<>()).computeIfAbsent(measure, k -> new CPUTimeBenchmark());
	}

	public long getLong(String group, String measure)
	{
		return (Long) groups.computeIfAbsent(group, s -> new HashMap<>()).computeIfAbsent(measure, k -> 0);
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

	private void set_(String measure, Object val)
	{
		set_(defaultGroup, measure, val);
	}

	private void set_(String group, String measure, Object val)
	{
		groups.computeIfAbsent(group, s -> new HashMap<>()).put(measure, val);
	}
	// ==========================================================================

	public void set(String measure, CPUTimeBenchmark val)
	{
		set_(measure, val);
	}

	public void set(String measureName, String set)
	{
		set_(measureName, set);
	}

	public void set(String measure, int val)
	{
		set_(measure, val);
	}

	public void set(String measure, int val[])
	{
		set_(measure, val);
	}
	// ==========================================================================

	public void set(String group, String measure, CPUTimeBenchmark val)
	{
		set_(group, measure, val);
	}

	public void set(String group, String measureName, String set)
	{
		set_(group, measureName, set);
	}

	public void set(String group, String measure, long val)
	{
		set_(group, measure, val);
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
			print.printf("\n[%s%s]\n", prefix, group.getKey());

			var sortedMeasures = new ArrayList<>(group.getValue().entrySet());
			Collections.sort(sortedMeasures, Map.Entry.<String, Object>comparingByKey());

			for (var measure : sortedMeasures)
				print.printf("%s: %s\n", measure.getKey(), measure.getValue());
		}
	}
}

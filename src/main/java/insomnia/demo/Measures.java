package insomnia.demo;

import java.util.ArrayList;
import java.util.List;

import insomnia.lib.cpu.CPUTimeBenchmark;

public final class Measures
{
	private List<CPUTimeBenchmark> measures;

	private Measures()
	{

	}

	public static Measures nbMeasures(int nb)
	{
		var ret = new Measures();
		ret.measures = new ArrayList<>(nb);

		while (nb-- > 0)
			ret.measures.add(new CPUTimeBenchmark());

		return ret;
	}

	public CPUTimeBenchmark get(int index)
	{
		return measures.get(index);
	}

	public CPUTimeBenchmark set(int index, CPUTimeBenchmark time)
	{
		return measures.set(index, time);
	}
}

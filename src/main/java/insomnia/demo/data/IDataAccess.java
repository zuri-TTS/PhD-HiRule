package insomnia.demo.data;

import java.io.PrintWriter;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import insomnia.data.ITree;
import insomnia.lib.cpu.CPUTimeBenchmark;

public interface IDataAccess<VAL, LBL>
{
	@FunctionalInterface
	public static interface UFunctionTransform<VAL, LBL>
	{
		Function<Object, ITree<VAL, LBL>> transform(Function<Object, ITree<VAL, LBL>> function);
	}

	Stream<ITree<VAL, LBL>> all();

	Stream<ITree<VAL, LBL>> execute(Stream<ITree<VAL, LBL>> queries, UFunctionTransform<VAL, LBL> userWrap, CPUTimeBenchmark firstEval);

	Stream<Pair<ITree<VAL, LBL>, Stream<ITree<VAL, LBL>>>> executeEach(Stream<ITree<VAL, LBL>> queries, UFunctionTransform<VAL, LBL> userWrap, CPUTimeBenchmark firstEval);

	void writeInfos(PrintWriter printer);
}

package insomnia.demo.data;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Triple;

import insomnia.data.ITree;
import insomnia.lib.cpu.CPUTimeBenchmark;

public interface IDataAccess<VAL, LBL>
{
	@FunctionalInterface
	public static interface UFunctionTransform<VAL, LBL>
	{
		Function<Object, ITree<VAL, LBL>> transform(Function<Object, ITree<VAL, LBL>> function);
	}

	public static interface explainStats
	{
		long getNbAnswers();

		CPUTimeBenchmark getTime();
	}

	Stream<ITree<VAL, LBL>> all();

	Stream<Object> explain(Stream<ITree<VAL, LBL>> queries);

	Stream<Object> execute(Stream<ITree<VAL, LBL>> queries);

	Stream<Object> executeNatives(Stream<Object> nativeQueries);

	Stream<Triple<ITree<VAL, LBL>, Object, Stream<Object>>> executeEach(Stream<ITree<VAL, LBL>> queries);

	ITree<VAL, LBL> nativeToTree(Object nativeRecord);

	Object treeToQNative(ITree<VAL, LBL> query);

	String getRecordId(Object record);

	explainStats explainStats(Object record);

	long getNbQueries();

	long getNbBatches();

	void encodeNativeQuery(Object query, PrintStream printer);

	Object decodeNativeQuery(String from);

	void writeInfos(PrintWriter printer);

}

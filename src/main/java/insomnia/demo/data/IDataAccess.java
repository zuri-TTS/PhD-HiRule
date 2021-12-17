package insomnia.demo.data;

import java.io.PrintWriter;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import insomnia.data.ITree;

public interface IDataAccess<VAL, LBL>
{
	@FunctionalInterface
	public static interface UFunctionTransform<VAL, LBL>
	{
		Function<Object, ITree<VAL, LBL>> transform(Function<Object, ITree<VAL, LBL>> function);
	}

	Stream<ITree<VAL, LBL>> all();

	Stream<Object> execute(Stream<ITree<VAL, LBL>> queries);

	Stream<Pair<ITree<VAL, LBL>, Stream<Object>>> executeEach(Stream<ITree<VAL, LBL>> queries);

	ITree<VAL, LBL> nativeToTree(Object nativeRecord);

	String getRecordId(Object record);

	void writeInfos(PrintWriter printer);
}

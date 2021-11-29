package insomnia.demo.data;

import java.io.PrintWriter;
import java.util.function.Function;
import java.util.stream.Stream;

import insomnia.data.ITree;

public interface IDataAccess<VAL, LBL>
{
	@FunctionalInterface
	public static interface UFunctionTransform<VAL, LBL>
	{
		Function<Object, ITree<VAL, LBL>> transform(Function<Object, ITree<VAL, LBL>> function);
	}

	Stream<ITree<VAL, LBL>> all();

	Stream<ITree<VAL, LBL>> execute(Stream<ITree<VAL, LBL>> queries, UFunctionTransform<VAL, LBL> userWrap);

	void writeInfos(PrintWriter printer);
}

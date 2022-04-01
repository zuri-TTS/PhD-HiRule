package insomnia.demo.input;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration2.Configuration;

import insomnia.data.ITree;
import insomnia.demo.Measures;
import insomnia.demo.TheConfiguration;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.input.InputData.Filters;
import insomnia.implem.kv.KV;
import insomnia.implem.kv.data.KVLabel;

public final class Query
{
	private Query()
	{
		throw new AssertionError();
	}

	public static ITree<Object, KVLabel> get(Configuration config) throws IOException, ParseException
	{
		return getOneFile(config.getString(TheConfiguration.OneProperty.Query.getPropertyName()));
	}

//	public static Stream<ITree<Object, KVLabel>> getAll(Configuration config) throws IOException, ParseException
//	{
//		String[] uris = config.getStringArray(TheConfiguration.OneProperty.Query.getPropertyName());
//
//		return null;
//	}

	public static ITree<Object, KVLabel> getOneFile(String uri) throws IOException, ParseException
	{
		var data = InputData.filters(InputData.getLinesOf(uri), Filters.NO_BLANK, Filters.NO_COMMENT) //
			.collect(Collectors.toList());

		if (data.size() == 0)
			throw new IllegalArgumentException("No input query");

		var s = data.stream().reduce((a, b) -> a + b).get();
		return KV.treeFromString(s);
	}

	public static boolean isNative(Configuration config)
	{
		var qnative = config.getString(TheConfiguration.OneProperty.QueryNative.getPropertyName(), "");

		return !qnative.isEmpty();
	}

	public static Stream<Object> getNatives(Configuration config, Measures measures) throws IOException, URISyntaxException, ParseException
	{
		var file       = InputData.tryOpenStream(config.getString(TheConfiguration.OneProperty.QueryNative.getPropertyName(), ""));
		var dataAccess = DataAccesses.getDataAccess(config, measures);

		return InputData.getLinesOf(file).map(dataAccess::decodeNativeQuery);
	}
}

package insomnia.demo.data;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

import insomnia.demo.data.mongodb.DataAccess;
import insomnia.implem.kv.data.KVLabel;

public final class DataAccesses
{
	private DataAccesses()
	{
		throw new AssertionError();
	}

	// ==========================================================================

	private static enum DataAccessFactory
	{
		THE_FACTORY;

		Map<String, BiFunction<URI, Configuration, IDataAccess<Object, KVLabel>>> i = Map.of( //
			"mongodb", DataAccess::open //
		);
	}

	private static enum DataAccessConfigProperties
	{
		THE_FACTORY;

		Map<String, Options> i = Map.of( //
			"mongodb", DataAccess.getItsConfigProperties() //
		);
	}

	public static IDataAccess<Object, KVLabel> getDataAccess(Configuration config) throws URISyntaxException
	{
		var dataUri = new URI(config.getString("data"));
		return DataAccessFactory.THE_FACTORY.i.get(dataUri.getScheme()).apply(dataUri, config);
	}

	public static Map<String, Options> getDataAccessConfigProperties()
	{
		return DataAccessConfigProperties.THE_FACTORY.i;
	}
}

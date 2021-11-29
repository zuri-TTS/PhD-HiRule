package insomnia.demo.data;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.function.Function;

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

		Map<String, Function<URI, IDataAccess<Object, KVLabel>>> i = Map.of( //
			"mongodb", DataAccess::open //
		);
	}

	public static IDataAccess<Object, KVLabel> getDataAccess(Configuration config) throws URISyntaxException
	{
		var dataUri = new URI(config.getString("data"));
		return DataAccessFactory.THE_FACTORY.i.get(dataUri.getScheme()).apply(dataUri);
	}

}

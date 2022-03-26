package insomnia.demo.data;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

import insomnia.demo.Measures;
import insomnia.demo.data.mongodb.DataAccess;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.help.HelpFunctions;

public final class DataAccesses
{
	private DataAccesses()
	{
		throw new AssertionError();
	}

	// ==========================================================================

	@FunctionalInterface
	private static interface Open
	{
		IDataAccess<Object, KVLabel> open(URI uri, Configuration config, String db, String collection, Measures measures);
	}

	private static enum DataAccessFactory
	{
		THE_FACTORY;

		Map<String, Open> i = Map.of( //
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

	public static Collection<IDataAccess<Object, KVLabel>> getDataAccesses(Configuration config)
	{
		var db          = config.getString("db");
		var collections = config.getList("db.collection");

		return collections.stream().map(HelpFunctions.avoidException(c -> getDataAccess(config, db, (String) c, new Measures()))).collect(Collectors.toList());
	}

	public static IDataAccess<Object, KVLabel> getDataAccess(Configuration config, Measures measures) throws URISyntaxException
	{
		return getDataAccess(config, config.getString("db"), config.getString("db.collection"), measures);
	}

	public static IDataAccess<Object, KVLabel> getDataAccess(Configuration config, String db, String collection, Measures measures) throws URISyntaxException
	{
		var dataUri = new URI(config.getString("data"));
		return DataAccessFactory.THE_FACTORY.i.get(dataUri.getScheme()).open(dataUri, config, db, collection, measures);
	}

	public static Map<String, Options> getDataAccessConfigProperties()
	{
		return DataAccessConfigProperties.THE_FACTORY.i;
	}
}

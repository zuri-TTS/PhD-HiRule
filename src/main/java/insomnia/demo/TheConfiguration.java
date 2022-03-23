package insomnia.demo;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.BasicConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import insomnia.lib.memoization.Memoizers;

public final class TheConfiguration
{
	private TheConfiguration()
	{
		throw new AssertionError();
	}

	// ==========================================================================

	private enum MyOptions
	{
		Query(Option.builder().longOpt("query").desc("Path to the query file").build()) //
		, QueryNative(Option.builder().longOpt("query.native").desc("Path to a file of native queries").build()) //
		, Rules(Option.builder().longOpt("rules").desc("Path to a file of rules or a directory of files").build()) //
		, Summary(Option.builder().longOpt("summary").desc("Path to the summary file, or empty if no summary").build()) //
		, SummaryType(Option.builder().longOpt("summary.type").desc("Type of the summary file: key|key-type|path").build()) //
		, SummaryFilterTypes(Option.builder().longOpt("summary.filter.types").desc("Use the 'NodeTypes' informations from the summary to filter more reformulations").build()) //
		;

		Option opt;

		private MyOptions(Option o)
		{
			opt = o;
		}
	}

	public static Options getConfigProperties()
	{
		var ret = new Options();

		for (var opt : List.of(MyOptions.values()))
			ret.addOption(opt.opt);

		return ret;
	}
	// ==========================================================================

	public enum OneProperty
	{
		Query(MyOptions.Query.opt.getLongOpt()) //
		, QueryNative(MyOptions.QueryNative.opt.getLongOpt()) //
		, Rules(MyOptions.Rules.opt.getLongOpt()) //
		, Summary(MyOptions.Summary.opt.getLongOpt()) //
		, SummaryType(MyOptions.SummaryType.opt.getLongOpt()) //
		, SummaryFilterTypes(MyOptions.SummaryFilterTypes.opt.getLongOpt()) //
		;

		String name;

		private OneProperty(String name)
		{
			this.name = name;
		}

		public String getPropertyName()
		{
			return name;
		}
	}

	// ==========================================================================

	public static Configuration union(List<? extends Configuration> configs) throws IOException, ParseException, ConfigurationException
	{
		CompositeConfiguration composite;

		composite = new CompositeConfiguration();
		configs.forEach(composite::addConfiguration);
		return config(composite);
	}

	public static Configuration getUnsafe(String uri)
	{
		try
		{
			return get(uri);
		}
		catch (IOException | ParseException | ConfigurationException e)
		{
			throw new Error(e);
		}
	}

	public static Configuration get(String uri) throws IOException, ParseException, ConfigurationException
	{
		if (!uri.isEmpty())
		{
			try
			{
				return config(new Configurations().properties(new URL(uri)));
			}
			catch (MalformedURLException e)
			{
				var path = Paths.get(uri);

				if (Files.exists(path))
					return config(new Configurations().properties(path.toFile()));

				throw new IllegalArgumentException(String.format("Can't read config `%s`", uri));
			}
		}
		return new BasicConfigurationBuilder<>(PropertiesConfiguration.class).getConfiguration();
	}

	private static AbstractConfiguration config(AbstractConfiguration config)
	{
		config.setListDelimiterHandler(new DefaultListDelimiterHandler(','));
		return config;
	}

	// ==========================================================================

	private static Supplier<Configuration> confDef = Memoizers.lazy(() -> {
		try
		{
			var url    = TheConfiguration.class.getClassLoader().getResource("insomnia/demo/config/default.properties");
			var config = new Configurations().properties(url);
			return config(config);
		}
		catch (ConfigurationException e)
		{
			throw new AssertionError();
		}
	});

	public static Configuration getDefault()
	{
		return confDef.get();
	}
}

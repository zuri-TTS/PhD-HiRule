package insomnia.demo.command;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.input.InputData;
import insomnia.demo.input.LogicalPartition;
import insomnia.lib.numeric.MultiInterval;

final class ComPartition implements ICommand
{
	private enum MyOptions
	{
		OutputPattern(Option.builder().longOpt("partition.output.pattern").desc("Output path for save results in files; %s must be in the pattern to be replaced by a name").build()) //
		, Mode(Option.builder().longOpt("partition.mode").desc("mode='prefix'").build()) //

		;

		Option opt;

		private MyOptions(Option o)
		{
			opt = o;
		}
	}

	@Override
	public Options getConfigProperties()
	{
		var ret = new Options();
		TheConfiguration.getConfigProperties().getOptions().forEach(ret::addOption);

		for (var opt : List.of(MyOptions.values()))
			ret.addOption(opt.opt);

		return ret;
	}

	@Override
	public String getName()
	{
		return "partition";
	}

	@Override
	public String getDescription()
	{
		return "Get infos for a logical paritionning of a collection";
	}

	private String outputPattern;

	private String getFileName(String file)
	{
		return file;
	}

	private PrintStream outputFilePrinter(String fileName, OpenOption... options) throws IOException
	{
		if (outputPattern == null)
			return new PrintStream(OutputStream.nullOutputStream());

		fileName = getFileName(fileName);
		var filePath = Path.of(String.format(outputPattern, fileName));

		return new PrintStream(InputData.tryOpenOutputPath(filePath, options));
	}

	// ==========================================================================

	private void prefixPartition(Configuration config) throws ParseException, URISyntaxException
	{
		var da = DataAccesses.getDataAccess(config, TheDemo.measures());

		var partitions       = config.getList(String.class, "partition", List.of());
		var partitionDecoder = LogicalPartition.decoder();

		for (var partition_str : partitions)
		{
			var idInterval = MultiInterval.empty();
			var partition  = partitionDecoder.decode(partition_str);
			var name       = partition.getName();

			da.all().map(da::getRecordId).forEach(idInterval::add);

			try (var out = outputFilePrinter(name))
			{
				out.println(idInterval);
			}
			catch (IOException e)
			{
				System.err.printf("Error writing %s\n", name);
				e.printStackTrace();
			}
		}
	}

	// ==========================================================================

	public void execute(Configuration config) throws Exception
	{
		outputPattern = config.getString(MyOptions.OutputPattern.opt.getLongOpt());
		var mode = config.getString(MyOptions.Mode.opt.getLongOpt());

		switch (mode)
		{
		case "prefix":
			prefixPartition(config);
			break;
		default:
			throw new IllegalArgumentException(String.format("Invalid mode '%s'", mode));
		}
	}
}

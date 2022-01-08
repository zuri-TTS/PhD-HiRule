package insomnia.demo.command;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.LabelSummaryWriter;
import insomnia.implem.summary.PathSummary;
import insomnia.implem.summary.PathSummaryWriter;
import insomnia.lib.codec.IEncoder;
import insomnia.summary.ISummary;

final class ComSummarize implements ICommand
{
	private enum MyOptions
	{
		SummaryType(Option.builder().longOpt("summary.type").desc("key|path").build()), //
		PrettyPrint(Option.builder().longOpt("summary.prettyPrint").build()), //
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

		for (var opt : List.of(MyOptions.values()))
			ret.addOption(opt.opt);

		return ret;
	}

	// ==========================================================================

	@Override
	public String getName()
	{
		return "summarize";
	}

	@Override
	public String getDescription()
	{
		return "Create the summary";
	}

	// ==========================================================================

	@Override
	public void execute(Configuration config)
	{
		var type = config.getString(MyOptions.SummaryType.opt.getLongOpt(), "key");

		ISummary<Object, KVLabel> summary;

		IEncoder<ISummary<Object, KVLabel>> encoder;

		try
		{
			TheDemo.out().println(type);

			switch (type)
			{
			case "key":
				summary = LabelSummary.create();
				encoder = (s, w) -> {
					new LabelSummaryWriter<Object, KVLabel>().setLabelEncoder(KVLabels::encodeTo) //
						.writeTo((LabelSummary<Object, KVLabel>) s, w);
				};
				break;
			case "path":
				summary = PathSummary.create();
				encoder = (s, w) -> {
					new PathSummaryWriter<Object, KVLabel>().setLabelEncoder(KVLabels::encodeTo) //
						.setPrettyPrint(config.getBoolean(MyOptions.PrettyPrint.opt.getLongOpt(), false)) //
						.writeTo((PathSummary<Object, KVLabel>) s, w);
				};
				break;
			default:
				throw new IllegalArgumentException(String.format("Invalid summary type: %s", type));
			}
			var dataAccess = DataAccesses.getDataAccess(config);
			var file       = Paths.get(config.getString("summary"));
			dataAccess.all().forEach(summary::addTree);

			try (var swriter = Files.newBufferedWriter(file))
			{
				encoder.encodeTo(summary, swriter);
				TheDemo.measure("summary", "depth", summary.getDepth());
				TheDemo.measure("summary", "labels", summary.nbLabels());
				TheDemo.measure("summary", "file", file.toString());
			}
		}
		catch (URISyntaxException | IOException e)
		{
			throw new IllegalArgumentException(e);
		}
	}

}

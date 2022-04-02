package insomnia.demo.command;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;

import insomnia.demo.Measures;
import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.input.LogicalPartition;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.LabelSummaryWriter;
import insomnia.implem.summary.LabelTypeSummary;
import insomnia.implem.summary.LabelTypeSummaryWriter;
import insomnia.implem.summary.PathSummary;
import insomnia.implem.summary.PathSummaryWriter;
import insomnia.lib.codec.IEncoder;
import insomnia.summary.ISummary;

final class ComSummarize implements ICommand
{
	private enum MyOptions
	{
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

	private Pair<ISummary<Object, KVLabel>, IEncoder<ISummary<Object, KVLabel>>> summaryAndEncoder(String type, boolean prettyPrint)
	{
		ISummary<Object, KVLabel> summary;

		IEncoder<ISummary<Object, KVLabel>> encoder;

		switch (type)
		{
		case "key":
			summary = LabelSummary.create();
			encoder = (s, w) -> {
				new LabelSummaryWriter<Object, KVLabel>().setLabelEncoder(KVLabels::encodeTo) //
					.writeTo((LabelSummary<Object, KVLabel>) s, w);
			};
			break;
		case "key-type":
			summary = LabelTypeSummary.create();
			encoder = (s, w) -> {
				new LabelTypeSummaryWriter<Object, KVLabel>().setLabelEncoder(KVLabels::encodeTo) //
					.writeTo((LabelTypeSummary<Object, KVLabel>) s, w);
			};
			break;
		case "path":
			summary = PathSummary.create();
			encoder = (s, w) -> {
				new PathSummaryWriter<Object, KVLabel>().setLabelEncoder(KVLabels::encodeTo) //
					.setPrettyPrint(prettyPrint) //
					.writeTo((PathSummary<Object, KVLabel>) s, w);
			};
			break;
		default:
			throw new IllegalArgumentException(String.format("Invalid summary type: %s", type));
		}
		return Pair.of(summary, encoder);
	}

	@Override
	public void execute(Configuration config)
	{
		var confPartitions = config.getList(String.class, "partition", List.of());

		if (confPartitions.isEmpty())
		{
			var summaryPath = config.getString("summary");
			executePartition(config, summaryPath, LogicalPartition.nullValue(), TheDemo.measures());
		}
		else
		{
			var pdecoder    = LogicalPartition.decoder();
			var confSummary = config.getList(String.class, "summary");

			int i = 0;
			for (var cp : confPartitions)
			{
				var summaryPath = confSummary.get(i++);

				try
				{
					var measures  = new Measures();
					var partition = pdecoder.decode(cp);

					measures.setPrefix(partition.getName() + "/");
					executePartition(config, summaryPath, partition, measures);
					TheDemo.measures().addAll(measures);
				}
				catch (ParseException e)
				{
					System.err.printf("Error decoding partition %s: %s", cp, e.getMessage());
				}
			}
		}
	}

	private void executePartition(Configuration config, String summaryPath, LogicalPartition partition, Measures measures)
	{
		try
		{
			var type = config.getString(TheConfiguration.OneProperty.SummaryType.getPropertyName(), "key");
			var SE   = summaryAndEncoder(type, config.getBoolean(MyOptions.PrettyPrint.opt.getLongOpt(), false));

			var summary = SE.getKey();
			var encoder = SE.getValue();

			var dataAccess = DataAccesses.getDataAccess(config, TheDemo.measures());
			dataAccess.setLogicalPartition(partition);

			dataAccess.all().forEach(summary::addTree);

			if (!summaryPath.isEmpty())
			{
				var filePath = Paths.get(summaryPath);

				try (var swriter = Files.newBufferedWriter(filePath))
				{
					encoder.encodeTo(summary, swriter);
				}
				measures.set("summary", "file", filePath.toString());
			}
			measures.set("summary", "depth", summary.getDepth());
			measures.set("summary", "labels", summary.nbLabels());
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException(e);
		}
	}

}

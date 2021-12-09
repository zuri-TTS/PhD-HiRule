package insomnia.demo.command;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.LabelSummaryWriter;

final class ComSummarize implements ICommand
{
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

	@Override
	public void execute(Configuration config)
	{
		try
		{
			var summary    = LabelSummary.<Object, KVLabel>create();
			var dataAccess = DataAccesses.getDataAccess(config);
			var file       = Paths.get(config.getString("summary"));
			dataAccess.all().forEach(summary::addTree);

			try (var swriter = Files.newBufferedWriter(file))
			{
				new LabelSummaryWriter<Object, KVLabel>(swriter).setWriteLabel(KVLabels::parseable).write(summary);
			}
			TheDemo.out().printf("Depth: %d, Labels: %d, file: `%s`", summary.getDepth(), summary.nbLabels(), file);
		}
		catch (URISyntaxException | IOException e)
		{
			throw new IllegalArgumentException(e);
		}
	}

}

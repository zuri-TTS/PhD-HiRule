package insomnia.demo.command;

import org.apache.commons.configuration2.Configuration;

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
//		TheDemo.out().println("Summarize");
//
//		try
//		{
//			var summary    = LabelSummary.<Object, KVLabel>create();
//			var dataAccess = getDataAccess(data);
//			var file       = Paths.get(data.getConfiguration().getString("summary"));
//			dataAccess.all().forEach(summary::addTree);
//
//			try (var swriter = Files.newBufferedWriter(file))
//			{
//				new LabelSummaryWriter<Object, KVLabel>(swriter).setWriteLabel(KVLabels::parseable).write(summary);
//			}
//			TheDemo.out().printf("Depth: %d, Labels: %d, file: `%s`", summary.getDepth(), summary.nbLabels(), file);
//		}
//		catch (URISyntaxException | IOException e)
//		{
//			throw new IllegalArgumentException(e);
//		}
	}

}

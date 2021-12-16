package insomnia.demo.command;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.bag.HashBag;
import org.apache.commons.configuration2.Configuration;

import insomnia.data.ITree;
import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.input.InputData;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.cpu.CPUTimeBenchmark;

final class ComQuerying implements ICommand
{
	private enum MyOptions
	{
		OutputPattern(Option.builder().longOpt("querying.output.pattern").desc("Output path for save results in files").build()) //
		, QueryEach(Option.builder().longOpt("querying.each").desc("(bool) Results query by query").build()) //
		, DisplayAnswers(Option.builder().longOpt("querying.display.answers").desc("(bool) Display the answers").build()) //
		, ConfigPrint(Option.builder().longOpt("querying.config.print").desc("(bool) Print the config in a file").build()) //
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

	@Override
	public String getName()
	{
		return "querying";
	}

	@Override
	public String getDescription()
	{
		return "Query database with reformulations";
	}

	// ==========================================================================

	private String outputPattern;

	private PrintStream outputFilePrinter(String fileName, OpenOption... options)
	{
		if (outputPattern == null)
			return new PrintStream(OutputStream.nullOutputStream());

		var filePath = Path.of(String.format(outputPattern, fileName));

		return new PrintStream(InputData.fakeOpenOutputPath(filePath, options));
	}

	// ==========================================================================

	private void executeEach(Configuration config) throws Exception
	{
		var dataAccess   = DataAccesses.getDataAccess(config);
		var resultStream = dataAccess.executeEach( //
			ComGenerate.queries(config), //
			TheDemo.measure("each.query.eval.stream.create"), //
			TheDemo.measure("each.query.query2native") //
		);

		List<ITree<Object, KVLabel>> emptyQueries    = new ArrayList<>();
		List<ITree<Object, KVLabel>> nonEmptyQueries = new ArrayList<>();
		Bag<String>                  allRecords      = new HashBag<>();

		int nbQueries[] = new int[1];
		{
			var qout    = new PrintStream(outputFilePrinter("each-qresults"));
			var qnempty = new PrintStream(outputFilePrinter("each-non-empty"));

			resultStream.forEach(p -> {
				nbQueries[0]++;
				var query   = p.getLeft();
				var records = p.getRight().map(dataAccess::getRecordId).collect(Collectors.toList());

				if (records.isEmpty())
					emptyQueries.add(query);
				else
				{
					nonEmptyQueries.add(query);
					qout.printf("\n\n%s\n", query.toString());
					records.forEach(qout::println);
					allRecords.addAll(records);
					qnempty.printf("%s\n%s\n\n", query, records.size());
				}
			});
			qout.close();
			qnempty.close();
		}
		TheDemo.measure("each.reformulations", "empty", emptyQueries.size());
		TheDemo.measure("each.reformulations", "non-empty", nbQueries[0] - emptyQueries.size());
		TheDemo.measure("each.reformulations", "total", nbQueries[0]);
		TheDemo.measure("each.answers", "total", allRecords.size());
		TheDemo.measure("each.answers", "unique", allRecords.uniqueSet().size());

		var printer = outputFilePrinter("each-empty");
		emptyQueries.forEach(printer::println);
		printer.close();

		printer = outputFilePrinter("each-answers");
		allRecords.forEach(printer::println);
		printer.close();

		printer = outputFilePrinter("each-answers-unique");
		allRecords.uniqueSet().forEach(printer::println);
		printer.close();

//		if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
//			ComConfig.print(config, outputFilePrinter("each-config"), true);

		executeBatch(config, nonEmptyQueries.stream());
	}

	public void execute(Configuration config) throws Exception
	{
		outputPattern = config.getString(MyOptions.OutputPattern.opt.getLongOpt());

		if (config.getBoolean(MyOptions.QueryEach.opt.getLongOpt(), false))
			executeEach(config);
		else
			executeBatch(config);
	}

	private void executeBatch(Configuration config) throws Exception
	{
		executeBatch(config, ComGenerate.queries(config));
	}

	private void executeBatch(Configuration config, Stream<ITree<Object, KVLabel>> queries) throws Exception
	{
		try
		{
			var createStreamMeas = TheDemo.measure("query.eval.stream.create");

			var dataAccess   = DataAccesses.getDataAccess(config);
			var resultStream = dataAccess.execute( //
				queries, //
				createStreamMeas, //
				TheDemo.measure("query.query2native") //
			);

			var         qeval      = TheDemo.measure("query.eval.total");
			var         qstream    = TheDemo.measure("query.eval.stream");
			Bag<String> allRecords = new HashBag<>();

			{
				var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);
				var ans_out        = new PrintStream( //
					displayAnswers ? outputFilePrinter("answers") : PrintStream.nullOutputStream() //
				);

				Consumer<String> displayProcess = displayAnswers //
					? r -> ans_out.println(r) //
					: r -> {
					};

				CPUTimeBenchmark.startChrono(qeval, qstream);
				{
					resultStream.forEach(r -> {
						qstream.stopChrono();
						var id = dataAccess.getRecordId(r);
						allRecords.add(id);
						displayProcess.accept(id);
						qstream.startChrono();
					});
				}
				CPUTimeBenchmark.stopChrono(qeval, qstream);
				ans_out.close();
			}
			TheDemo.measure("query.eval.stream.action", CPUTimeBenchmark.minus(qeval, qstream));
			TheDemo.measure("answers", "total", allRecords.size());
			TheDemo.measure("answers", "unique", allRecords.uniqueSet().size());
			qeval.plus(createStreamMeas);

			if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
				ComConfig.print(config, outputFilePrinter("config"), true);
		}
		catch (URISyntaxException e)
		{
			throw new IllegalArgumentException(e);
		}
	}
}

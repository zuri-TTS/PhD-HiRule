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
import insomnia.demo.data.IDataAccess.UFunctionTransform;
import insomnia.demo.input.InputData;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.lib.cpu.CPUTimeBenchmark;

final class ComQuerying implements ICommand
{
	private enum MyOptions
	{
		OutputPattern(Option.builder().longOpt("querying.output.pattern").desc("Output path for save results in files").build()) //
		, QueryEach(Option.builder().longOpt("querying.each").desc("(bool) Results query by query").build()) //
		, DisplayAnswers(Option.builder().longOpt("querying.display.answers").desc("(bool) Display the answers").build()) //
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

	private UFunctionTransform<Object, KVLabel> userWrap()
	{
		var qtrans = TheDemo.measure("query.tree2native");

		return f -> o -> {
			qtrans.startChrono();
			var ret = f.apply(o);
			qtrans.stopChrono();
			return ret;
		};
	}

	private String recordId(ITree<Object, KVLabel> record)
	{
		var val = (String) ITree.followLabel(record, record.getRoot(), KVLabels.create("_id")).get(0).getValue();
		return val;
	}

	private void executeEach(Configuration config) throws Exception
	{
		var dataAccess   = DataAccesses.getDataAccess(config);
		var resultStream = dataAccess.executeEach(ComGenerate.queries(config), userWrap(), TheDemo.measure("each.query.eval.stream.create"));

		List<ITree<Object, KVLabel>> emptyQueries = new ArrayList<>();
		Bag<String>                  allRecords   = new HashBag<>();

		int nbQueries[] = new int[1];
		{
			var qout    = new PrintStream(outputFilePrinter("qresults"));
			var qnempty = new PrintStream(outputFilePrinter("non-empty"));

			resultStream.forEach(p -> {
				nbQueries[0]++;
				var query   = p.getLeft();
				var records = p.getRight().map(this::recordId).collect(Collectors.toList());

				if (records.isEmpty())
					emptyQueries.add(query);
				else
				{
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

		var printer = outputFilePrinter("empty");
		emptyQueries.forEach(printer::println);
		printer.close();

		printer = outputFilePrinter("answers");
		allRecords.forEach(printer::println);
		printer.close();

		printer = outputFilePrinter("answers-unique");
		allRecords.uniqueSet().forEach(printer::println);
		printer.close();
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
			var resultStream = dataAccess.execute(queries, userWrap(), createStreamMeas);

			int count[] = new int[1];

			var qeval   = TheDemo.measure("query.eval.total");
			var qstream = TheDemo.measure("query.eval.stream");

			{
				var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);
				var ans_out        = new PrintStream( //
					displayAnswers ? outputFilePrinter("answers") : PrintStream.nullOutputStream() //
				);

				Consumer<ITree<Object, KVLabel>> displayProcess = displayAnswers //
					? r -> ans_out.println(recordId(r)) //
					: r -> {
					};

				CPUTimeBenchmark.startChrono(qeval, qstream);
				{
					resultStream.forEach(r -> {
						qstream.stopChrono();
						count[0]++;
						displayProcess.accept(r);
						qstream.startChrono();
					});
				}
				CPUTimeBenchmark.stopChrono(qeval, qstream);
				ans_out.close();
			}
			TheDemo.measure("query.eval.stream.action", CPUTimeBenchmark.minus(qeval, qstream));
			TheDemo.measure("answers", "total", count[0]);
			qeval.plus(createStreamMeas);
		}
		catch (URISyntaxException e)
		{
			throw new IllegalArgumentException(e);
		}
	}
}

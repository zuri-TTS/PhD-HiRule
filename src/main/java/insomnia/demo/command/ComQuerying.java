package insomnia.demo.command;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
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
import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.data.IDataAccess;
import insomnia.demo.input.InputData;
import insomnia.demo.input.Query;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.cpu.CPUTimeBenchmark;
import insomnia.lib.function.ProcessFunction;

final class ComQuerying implements ICommand
{
	private enum MyOptions
	{
		OutputPattern(Option.builder().longOpt("output.pattern").desc("Output path for save results in files; %s must be in the pattern to be replaced by a name").build()) //
		, QueryMode(Option.builder().longOpt("querying.mode").desc("(query|explain|each) Query mode").build()) //
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
		TheConfiguration.getConfigProperties().getOptions().forEach(ret::addOption);

		for (var opt : List.of(MyOptions.values()))
			ret.addOption(opt.opt);

		for (var entry : DataAccesses.getDataAccessConfigProperties().entrySet())
			for (var opt : entry.getValue().getOptions())
				ret.addOption(new Option(null, String.format("[%s]%s", entry.getKey(), opt.getLongOpt()), false, opt.getDescription()));

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

	private enum QueryMode
	{
		EACH, STATS, QUERY, EXPLAIN;

		static QueryMode fromString(String mode)
		{
			return QueryMode.valueOf(mode.toUpperCase());
		}
	}

	private String outputPattern;

	private String getFileName(String file)
	{
		if (TheDemo.measureHasPrefix())
			return TheDemo.getMeasurePrefix() + "-" + file;

		return file;
	}

	private PrintStream outputFilePrinter(String fileName, OpenOption... options)
	{
		if (outputPattern == null)
			return new PrintStream(OutputStream.nullOutputStream());

		fileName = getFileName(fileName);
		var filePath = Path.of(String.format(outputPattern, fileName));

		return new PrintStream(InputData.fakeOpenOutputPath(filePath, options));
	}

	// ==========================================================================

	private void executeEach(Configuration config) throws Exception
	{
		TheDemo.setMeasurePrefix("each");

		var create     = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_CREATE);
		var dataAccess = DataAccesses.getDataAccess(config);
		var queries    = ComGenerate.queries(config);

		create.startChrono();
		var resultStream = dataAccess.executeEach(queries);
		create.stopChrono();

		var qeval      = TheDemo.measure(TheDemo.TheMeasures.QEVAL_TOTAL);
		var strmTotal  = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_TOTAL);
		var strmNext   = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_NEXT);
		var strmAction = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_ACTION);

		Bag<String> allRecords = new HashBag<>();

		int nbQueries[] = new int[1];
		int nbEmpties[] = new int[1];
		{
			var qout          = new PrintStream(outputFilePrinter("qresults"));
			var qempty        = new PrintStream(outputFilePrinter("query-empty"));
			var qnempty       = new PrintStream(outputFilePrinter("query-non-empty"));
			var qnativeempty  = new PrintStream(outputFilePrinter("native-empty"));
			var qnativenempty = new PrintStream(outputFilePrinter("native-non-empty"));

			CPUTimeBenchmark.startChrono(qeval, strmNext, strmTotal);
			{
				resultStream.forEach(p -> {
					strmNext.stopChrono();
					strmAction.startChrono();
					nbQueries[0]++;
					var query   = p.getLeft();
					var nativeQ = p.getMiddle();
					var records = p.getRight().map(dataAccess::getRecordId).collect(Collectors.toList());

					if (records.isEmpty())
					{
						nbEmpties[0]++;
						dataAccess.encodeNativeQuery(nativeQ, qnativeempty);
						qempty.println(query);
					}
					else
					{
						qout.printf("\n\n%s\n", query.toString());
						records.forEach(qout::println);
						allRecords.addAll(records);
						dataAccess.encodeNativeQuery(nativeQ, qnativenempty);
						qnempty.printf("%s\n%s\n\n", query, records.size());
					}
					strmAction.stopChrono();
					strmNext.startChrono();
				});
			}
			CPUTimeBenchmark.stopChrono(qeval, strmNext, strmTotal);

			qeval.plus(create);
			strmTotal.plus(create);

			qout.close();
			qempty.close();
			qnempty.close();
			qnativeempty.close();
			qnativenempty.close();
		}
		TheDemo.measure("reformulations", "empty", nbEmpties[0]);
		TheDemo.measure("reformulations", "non-empty", nbQueries[0] - nbEmpties[0]);
		TheDemo.measure("reformulations", "total", nbQueries[0]);
		TheDemo.measure("answers", "total", allRecords.size());
		TheDemo.measure("answers", "unique", allRecords.uniqueSet().size());

		PrintStream printer;

		printer = outputFilePrinter("answers");
		allRecords.forEach(printer::println);
		printer.close();

		printer = outputFilePrinter("answers-unique");
		allRecords.uniqueSet().forEach(printer::println);
		printer.close();

		if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
			ComConfig.print(config, outputFilePrinter("config"), true);
	}

	private void stats(Configuration config) throws Exception
	{
		var dataAccess = DataAccesses.getDataAccess(config);
		var queries    = ComGenerate.queries(config);

		int nbQueries[] = new int[1];
		int nbEmpties[] = new int[1];
		{
			var qnativeempty  = new PrintStream(outputFilePrinter("native-empty"));
			var qnativenempty = new PrintStream(outputFilePrinter("native-non-empty"));
			{
				queries.forEach(q -> {
					PrintStream printer;

					boolean hasAnswer = dataAccess.hasAnswer(q);

					if (hasAnswer)
						printer = qnativenempty;
					else
					{
						printer = qnativeempty;
						nbEmpties[0]++;
					}

					dataAccess.encodeNativeQuery(dataAccess.treeToQNative(q), printer);
					nbQueries[0]++;
				});
			}
			qnativeempty.close();
			qnativenempty.close();
		}
		TheDemo.measure("stats", "documents.nb", (int) dataAccess.getNbDocuments());
		TheDemo.measure("stats", "queries.nb", nbQueries[0]);
		TheDemo.measure("stats", "queries.empty.nb", nbEmpties[0]);
		TheDemo.measure("stats", "queries.nonempty.nb", nbQueries[0] - nbEmpties[0]);

		if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
			ComConfig.print(config, outputFilePrinter("config"), true);
	}

	public void execute(Configuration config) throws Exception
	{
		outputPattern = config.getString(MyOptions.OutputPattern.opt.getLongOpt());

		switch (QueryMode.fromString(config.getString(MyOptions.QueryMode.opt.getLongOpt())))
		{
		case EACH:
			executeEach(config);
			break;
		case STATS:
			stats(config);
			break;
		case QUERY:
			if (Query.isNative(config))
				executeNative(config);
			else
				executeBatch(config);
			break;
		case EXPLAIN:
			explainBatch(config);
			break;
		}
	}

	private void executeNative(Configuration config) throws Exception
	{
		var create = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_CREATE);

		var qnatives   = Query.getNatives(config);
		var dataAccess = DataAccesses.getDataAccess(config);

		create.startChrono();
		var resultStream = dataAccess.executeNatives(qnatives);
		create.stopChrono();

		executeBatch(config, dataAccess, resultStream);
	}
	// ==========================================================================

	private void explainBatch(Configuration config) throws Exception
	{
		explainBatch(config, ComGenerate.queries(config));
	}

	private void explainBatch(Configuration config, Stream<ITree<Object, KVLabel>> queries) throws Exception
	{
		var create     = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_CREATE);
		var dataAccess = DataAccesses.getDataAccess(config);
		create.startChrono();
		var resultStream = dataAccess.explain(queries);
		create.stopChrono();
		explainBatch(config, dataAccess, resultStream);
	}

	private void explainBatch(Configuration config, IDataAccess<Object, KVLabel> dataAccess, Stream<Object> resultStream) throws Exception
	{
		var strmNext   = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_NEXT);
		var strmAction = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_ACTION);
		int nbAnsw[]   = { 0 };
		var dbTime     = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STATS_DB_TIME);

		executeBatch(config, dataAccess, resultStream, r -> {
			strmNext.stopChrono();
			strmAction.startChrono();
			var stats = dataAccess.explainStats(r);
			nbAnsw[0] += stats.getNbAnswers();
			dbTime.plus(stats.getTime());
			TheDemo.measure(TheDemo.TheMeasures.QEVAL_STATS_DB_TIME.getName() + dataAccess.getNbBatches(), stats.getTime());
			strmAction.stopChrono();
			strmNext.startChrono();
		}, () -> {
			TheDemo.measure("answers", "total", nbAnsw[0]);
		});
	}

	// ==========================================================================

	private void executeBatch(Configuration config) throws Exception
	{
		executeBatch(config, ComGenerate.queries(config));
	}

	private void executeBatch(Configuration config, Stream<ITree<Object, KVLabel>> queries) throws Exception
	{
		var create     = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_CREATE);
		var dataAccess = DataAccesses.getDataAccess(config);
		create.startChrono();
		var resultStream = dataAccess.execute(queries);
		create.stopChrono();
		executeBatch(config, dataAccess, resultStream);
	}

	private void executeBatch(Configuration config, IDataAccess<Object, KVLabel> dataAccess, Stream<Object> resultStream) throws Exception
	{
		var strmNext   = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_NEXT);
		var strmAction = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_ACTION);

		var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);
		var ans_out        = new PrintStream( //
			displayAnswers ? outputFilePrinter("answers") : PrintStream.nullOutputStream() //
		);

		Consumer<String> displayProcess = displayAnswers //
			? r -> ans_out.println(r) //
			: r -> {
			};

		Bag<String> allRecords = new HashBag<>();

		executeBatch(config, dataAccess, resultStream, r -> {
			strmNext.stopChrono();
			strmAction.startChrono();
			var id = dataAccess.getRecordId(r);
			allRecords.add(id);
			displayProcess.accept(id);
			strmAction.stopChrono();
			strmNext.startChrono();
		}, () -> {
			ans_out.close();
			TheDemo.measure("answers", "total", allRecords.size());
			TheDemo.measure("answers", "unique", allRecords.uniqueSet().size());
		});
	}

	// ==========================================================================

	private void executeBatch(Configuration config, IDataAccess<Object, KVLabel> dataAccess, Stream<Object> resultStream, Consumer<Object> resultAction, ProcessFunction end) throws Exception
	{
		TheDemo.setMeasurePrefix("");

		var qeval     = TheDemo.measure(TheDemo.TheMeasures.QEVAL_TOTAL);
		var strmTotal = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_TOTAL);
		var strmNext  = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_NEXT);

		{
			CPUTimeBenchmark.startChrono(qeval, strmNext, strmTotal);
			{
				resultStream.forEach(resultAction);
			}
			CPUTimeBenchmark.stopChrono(qeval, strmNext, strmTotal);

			var create = TheDemo.measure(TheDemo.TheMeasures.QEVAL_STREAM_CREATE);
			qeval.plus(create);
			strmTotal.plus(create);

			end.process();
		}
		TheDemo.measure("queries", "total", (int) dataAccess.getNbQueries());
		TheDemo.measure("queries", "batch.nb", (int) dataAccess.getNbBatches());

		if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
			ComConfig.print(config, outputFilePrinter("config"), true);
	}
}

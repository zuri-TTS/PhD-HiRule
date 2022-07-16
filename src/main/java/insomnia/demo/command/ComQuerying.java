package insomnia.demo.command;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.bag.HashBag;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;

import insomnia.data.ITree;
import insomnia.demo.Measures;
import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.data.IDataAccess;
import insomnia.demo.input.InputData;
import insomnia.demo.input.LogicalPartition;
import insomnia.demo.input.Query;
import insomnia.demo.input.Summary;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.cpu.CPUTimeBenchmark;
import insomnia.lib.function.ProcessFunction;

final class ComQuerying implements ICommand
{
	private enum MyOptions
	{
		OutputPattern(Option.builder().longOpt("output.pattern").desc("Output path for save results in files; %s must be in the pattern to be replaced by a name").build()) //
		, QueryMode(Option.builder().longOpt("querying.mode").desc("(each|stats|query|explain|explaincolls) Query mode").build()) //
		, QueryFilter(Option.builder().longOpt("querying.filter").desc("(empty|noempty) Filter queries that are only empty xor not").build()) //
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
	private enum QueryFilter
	{
		NONE, EMPTY, NOEMPTY;

		static QueryFilter fromString(String mode)
		{
			if (mode.isEmpty())
				return NONE;

			return QueryFilter.valueOf(mode.toUpperCase());
		}
	}

	private enum QueryMode
	{
		EACH, STATS, QUERY, EXPLAIN, EXPLAINCOLLS;

		static QueryMode fromString(String mode)
		{
			return QueryMode.valueOf(mode.toUpperCase());
		}
	}

	private String outputPattern;

	private String getFileName(String file)
	{
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

	private QueryFilter getQueryFilterE(Configuration config)
	{
		return QueryFilter.fromString(config.getString(MyOptions.QueryFilter.opt.getLongOpt()));
	}

	private Predicate<ITree<Object, KVLabel>> getQueryFilter(QueryFilter filterType, IDataAccess<Object, KVLabel> dataAccess)
	{
		switch (filterType)
		{
		case NONE:
			return null;
		case EMPTY:
			return t -> !dataAccess.hasAnswer(t);
		case NOEMPTY:
			return t -> dataAccess.hasAnswer(t);
		default:
			throw new AssertionError();
		}
	}

	private void assignQueryFilter(QueryFilter filterType, IDataAccess<Object, KVLabel> dataAccess)
	{
		var qfilter = getQueryFilter(filterType, dataAccess);

		if (null != qfilter)
			dataAccess.setQueryFilter(qfilter);
	}

	private IDataAccess<Object, KVLabel> getDataAccess(Configuration config, Measures measures) throws URISyntaxException, ParseException
	{
		var ret = DataAccesses.getDataAccess(config, measures);
		assignQueryFilter(getQueryFilterE(config), ret);
		return ret;
	}

	private Collection<IDataAccess<Object, KVLabel>> getDataAccesses(Configuration config)
	{
		var ret         = DataAccesses.getDataAccesses(config);
		var qfilterType = getQueryFilterE(config);

		if (qfilterType != QueryFilter.NONE)
			for (var da : ret)
				assignQueryFilter(qfilterType, da);

		return ret;
	}

	// ==========================================================================

	private void executeEach(Configuration config, Measures measures) throws Exception
	{
		measures.setPrefix("each");

		var create     = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_CREATE.measureName());
		var dataAccess = getDataAccess(config, measures);
		var queries    = ComGenerate.queries(config, measures);

		create.startChrono();
		var resultStream = dataAccess.executeEach(queries);
		create.stopChrono();

		var qeval      = measures.getTime(TheDemo.TheMeasures.QEVAL_TOTAL.measureName());
		var strmTotal  = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_TOTAL.measureName());
		var strmNext   = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_NEXT.measureName());
		var strmAction = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_ACTION.measureName());

		Bag<Long> allRecords = new HashBag<>();

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
		measures.set("reformulations", "empty", nbEmpties[0]);
		measures.set("reformulations", "non-empty", nbQueries[0] - nbEmpties[0]);
		measures.set("reformulations", "total", nbQueries[0]);
		measures.set("answers", "total", allRecords.size());
		measures.set("answers", "unique", allRecords.uniqueSet().size());

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

	private void stats(Configuration config, Measures measures) throws Exception
	{
		var dataAccess = getDataAccess(config, measures);
		var queries    = ComGenerate.queries(config, measures);

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
		measures.set("stats", "documents.nb", (int) dataAccess.getNbDocuments());
		measures.set("stats", "queries.nb", nbQueries[0]);
		measures.set("stats", "queries.empty.nb", nbEmpties[0]);
		measures.set("stats", "queries.nonempty.nb", nbQueries[0] - nbEmpties[0]);

		if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
			ComConfig.print(config, outputFilePrinter("config"), true);
	}

	public void execute(Configuration config) throws Exception
	{
		var measures = TheDemo.measures();

		outputPattern = config.getString(MyOptions.OutputPattern.opt.getLongOpt());

		switch (QueryMode.fromString(config.getString(MyOptions.QueryMode.opt.getLongOpt())))
		{
		case EACH:
			executeEach(config, measures);
			break;
		case STATS:
			stats(config, measures);
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
		case EXPLAINCOLLS:
			explainColls(config);
			break;
		}
	}
	// ==========================================================================

	private void executeNative(Configuration config) throws Exception
	{
		executeNative(config, TheDemo.measures());
	}

	private void executeNative(Configuration config, Measures measures) throws Exception
	{
		var create = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_CREATE.measureName());

		var qnatives   = Query.getNatives(config, measures);
		var dataAccess = getDataAccess(config, measures);

		create.startChrono();
		var resultStream = dataAccess.executeNatives(qnatives);
		create.stopChrono();

		executeBatch(config, measures, dataAccess, resultStream);
	}
	// ==========================================================================

	private void explainColls(Configuration config) throws IOException, ParseException, InterruptedException
	{
		var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);
		var measures       = TheDemo.measures();
		var summaries      = config.getList(String.class, "summary");
		var partitions     = config.getList(String.class, "partition");
		var dataAccesses   = getDataAccesses(config);
		var nbThreads      = dataAccesses.size();
		var reformulations = ComGenerate.getReformulations(config, measures);
		int i              = 0;

		var callables = new ArrayList<Callable<Pair<Measures, List<Object>>>>(nbThreads);
		var pdecoder  = LogicalPartition.decoder();

		for (var dataAccess : dataAccesses)
		{
			var              summary       = Summary.get(config, summaries.get(i));
			var              confPartition = partitions.get(i);
			LogicalPartition partition;
			i++;

			if (!confPartition.isEmpty())
			{
				partition = pdecoder.decode(confPartition);
				dataAccess.setLogicalPartition(partition);
			}
			else
				partition = dataAccess.getLogicalPartition();

			callables.add(() -> {
				var threadMeasures = new Measures();
				{
					var prefixBuilder = new StringBuilder();
					prefixBuilder.append(dataAccess.getCollectionName()).append('/');

					var pname = partition.getName();

					if (!pname.isEmpty())
						prefixBuilder.append(pname).append('/');

					threadMeasures.setPrefix(prefixBuilder.toString());
				}
				var threadTime = threadMeasures.getTime("thread", "time");

				threadTime.startChrono();

				var filteredRefs = ComGenerate.queries(reformulations, summary, threadMeasures);

				var create = threadMeasures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_CREATE.measureName());

				create.startChrono();
				var resultStream = dataAccess.explain(filteredRefs);
				create.stopChrono();

				var res = resultStream.collect(Collectors.toList());
				resultStream = res.stream();
				explainBatch(config, threadMeasures, dataAccess, resultStream);
				threadTime.stopChrono();
				return Pair.of(threadMeasures, res);
			});
		}

		var threadMeas  = measures.getTime("threads.time");
		var execService = Executors.newFixedThreadPool(nbThreads);

		threadMeas.startChrono();
		var futures = execService.invokeAll(callables);
		threadMeas.stopChrono();

		execService.shutdown();

		try
		{
			var answers_nb = 0;
			var queries_nb = 0;

			var output = outputFilePrinter("result");
			i = 1;

			for (var f : futures)
			{
				var res           = f.get();
				var threadMeasure = res.getKey();

				if (displayAnswers)
				{
					var result = res.getRight();
					output.println(String.format("Thread %d", i++));
					result.forEach(output::println);
					output.println();
				}
				measures.addAll(threadMeasure);
				answers_nb += threadMeasure.getInt("answers", "total");
				queries_nb += threadMeasure.getInt("queries", "total");
			}
			measures.set("answers", "total", answers_nb);
			measures.set("queries", "total", queries_nb);
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new AssertionError(e);
		}
		measures.set("threads", "nb", nbThreads);
	}

	// ==========================================================================

	private void explainBatch(Configuration config) throws Exception
	{
		explainBatch(config, TheDemo.measures());
	}

	private void explainBatch(Configuration config, Measures measures) throws Exception
	{
		explainBatch(config, measures, ComGenerate.queries(config, measures));
	}

	private void explainBatch(Configuration config, Measures measures, Stream<ITree<Object, KVLabel>> queries) throws Exception
	{
		var create     = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_CREATE.measureName());
		var dataAccess = getDataAccess(config, measures);

		create.startChrono();
		var resultStream = dataAccess.explain(queries);
		create.stopChrono();
		explainBatch(config, measures, dataAccess, resultStream);
	}

	private void explainBatch(Configuration config, Measures measures, IDataAccess<Object, KVLabel> dataAccess, Stream<Object> resultStream) throws Exception
	{
		var strmNext   = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_NEXT.measureName());
		var strmAction = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_ACTION.measureName());
		int nbAnsw[]   = { 0 };
		var dbTime     = measures.getTime(TheDemo.TheMeasures.QEVAL_STATS_DB_TIME.measureName());
		var output     = outputFilePrinter("result");

		var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);

		executeBatch(config, measures, dataAccess, resultStream, r -> {
			strmNext.stopChrono();
			strmAction.startChrono();
			var stats = dataAccess.explainStats(r);
			nbAnsw[0] += stats.getNbAnswers();
			dbTime.plus(stats.getTime());

			if (displayAnswers)
			{
				output.println(r);
			}
			strmAction.stopChrono();
			strmNext.startChrono();
		}, () -> {
			measures.set("answers", "total", nbAnsw[0]);
		});
	}

	// ==========================================================================

	private void executeBatch(Configuration config) throws Exception
	{
		executeBatch(config, TheDemo.measures());
	}

	private void executeBatch(Configuration config, Measures measures) throws Exception
	{
		executeBatch(config, measures, ComGenerate.queries(config, measures));
	}

	private void executeBatch(Configuration config, Measures measures, Stream<ITree<Object, KVLabel>> queries) throws Exception
	{
		var create = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_CREATE.measureName());

		var dataAccess = DataAccesses.getDataAccess(config, measures);
		create.startChrono();
		var resultStream = dataAccess.execute(queries);
		create.stopChrono();

		executeBatch(config, measures, dataAccess, resultStream);
	}

	private void executeBatch(Configuration config, Measures measures, IDataAccess<Object, KVLabel> dataAccess, Stream<Object> resultStream) throws Exception
	{
		var strmNext   = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_NEXT.measureName());
		var strmAction = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_ACTION.measureName());

		var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);
		var ans_out        = new PrintStream( //
			displayAnswers ? outputFilePrinter("answers") : PrintStream.nullOutputStream() //
		);

		Consumer<Long> displayProcess = displayAnswers //
			? r -> ans_out.println(r) //
			: r -> {
			};

		Bag<Long> allRecords = new HashBag<>();

		executeBatch(config, measures, dataAccess, resultStream, r -> {
			strmNext.stopChrono();
			strmAction.startChrono();
			var id = dataAccess.getRecordId(r);
			allRecords.add(id);
			displayProcess.accept(id);
			strmAction.stopChrono();
			strmNext.startChrono();
		}, () -> {
			ans_out.close();
			measures.set("answers", "total", allRecords.size());
			measures.set("answers", "unique", allRecords.uniqueSet().size());
		});
	}

	// ==========================================================================

	private void executeBatch(Configuration config, Measures measures, IDataAccess<Object, KVLabel> dataAccess, Stream<Object> resultStream, Consumer<Object> resultAction, ProcessFunction end) throws Exception
	{
		var qeval     = measures.getTime(TheDemo.TheMeasures.QEVAL_TOTAL.measureName());
		var strmTotal = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_TOTAL.measureName());
		var strmNext  = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_NEXT.measureName());

		{
			CPUTimeBenchmark.startChrono(qeval, strmNext, strmTotal);
			{
				resultStream.forEach(resultAction);
			}
			CPUTimeBenchmark.stopChrono(qeval, strmNext, strmTotal);

			var create = measures.getTime(TheDemo.TheMeasures.QEVAL_STREAM_CREATE.measureName());
			qeval.plus(create);
			strmTotal.plus(create);

			end.process();
		}
		measures.set("queries", "total", (int) dataAccess.getNbQueries());
		measures.set("queries", "batch.nb", (int) dataAccess.getNbBatches());

		if (config.getBoolean(MyOptions.ConfigPrint.opt.getLongOpt(), false))
			ComConfig.print(config, outputFilePrinter("config"), true);
	}
}

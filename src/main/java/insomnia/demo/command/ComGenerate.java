package insomnia.demo.command;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.configuration2.Configuration;

import insomnia.data.ITree;
import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.TheDemo.TheMeasures;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.input.InputData;
import insomnia.demo.input.Query;
import insomnia.demo.input.Rules;
import insomnia.demo.input.Summary;
import insomnia.fsa.fta.IBUFTA;
import insomnia.implem.data.Trees;
import insomnia.implem.fsa.fta.buftachunk.modifier.BUFTATerminalRuleApplier;
import insomnia.implem.fsa.fta.buftachunk.modifier.IBUFTAChunkModifier;
import insomnia.implem.fsa.fta.creational.BUFTABuilder;
import insomnia.implem.kv.KV;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.cpu.CPUTimeBenchmark;
import insomnia.lib.help.HelpStream;
import insomnia.rule.IRule;

final class ComGenerate implements ICommand
{
	private enum MyOptions
	{
		OutputPattern(Option.builder().longOpt("output.pattern").desc("Output path for save results in files; %s must be in the pattern to be replaced by a name").build()) //
		, Deduplicate(Option.builder().longOpt("rewritings.deduplicate").desc("Deduplicate the rewritings").build()) //

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
		return "generate";
	}

	@Override
	public String getDescription()
	{
		return "Generate all query reformulations";
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

	private static IBUFTAChunkModifier<Object, KVLabel> rewriteMod(Collection<? extends IRule<Object, KVLabel>> rules, CPUTimeBenchmark meas)
	{
		return c -> {
			var mod = BUFTATerminalRuleApplier.getMod(rules);
			meas.startChrono();
			mod.accept(c);
			meas.stopChrono();
		};
	}

	private static IBUFTA<Object, KVLabel> getReformulations(Configuration config) throws IOException, ParseException
	{
		var query = Query.get(config);
		var rules = Rules.get(config);

		var rewriteMeas = TheDemo.measure(TheDemo.TheMeasures.QREWR_RULES_APPLY);
		var totalMeas   = TheDemo.measure(TheDemo.TheMeasures.QREWR_RULES_TOTAL);

		var builder = BUFTABuilder.create(query, KV.fsaInterpretation()) //
			.setChunkModifier(rewriteMod(rules, rewriteMeas));

		totalMeas.startChrono();
		var ret = builder.create();
		totalMeas.stopChrono();

		return ret;
	}

	// ==========================================================================

	public static Stream<ITree<Object, KVLabel>> queries(Configuration config) throws IOException, ParseException
	{
		var summaryCreate = TheDemo.measure(TheMeasures.SUMMARY_CREATE);

		var reformulations = getReformulations(config);

		summaryCreate.startChrono();
		var summary = Summary.get(config);
		summaryCreate.stopChrono();

		Stream<ITree<Object, KVLabel>> st;

		if (summary.isEmpty())
			st = Trees.treesFromAutomatonStream(reformulations, 10);
		else
		{
			summary.consider(reformulations);
			st = summary.generateTrees();
		}
		var rewritingsGen = TheDemo.measure(TheMeasures.QREWR_GENERATION);
		return HelpStream.clamp(st, rewritingsGen::startChrono, rewritingsGen::stopChrono);
	}

	// ==========================================================================

	public void execute(Configuration config) throws Exception
	{
		outputPattern = config.getString(MyOptions.OutputPattern.opt.getLongOpt());
		var deduplicate = config.getBoolean(MyOptions.Deduplicate.opt.getLongOpt(), false);

		int nb[]   = new int[] { 0 };
		var access = DataAccesses.getDataAccess(config);

		var q = queries(config);

		var queries = new PrintStream(outputFilePrinter("queries"));
		var natives = new PrintStream(outputFilePrinter("natives"));

		if (deduplicate)
		{
			var queryDup = new ArrayListValuedHashMap<Object, Object>();

			q.forEach(t -> {
				var qnative = access.treeToQNative(t);
				queries.println(t);
				natives.println(qnative);

				t = Trees.create(t);
				queryDup.put(t, qnative);

				nb[0]++;
			});

			var dup = new PrintStream(outputFilePrinter("uniques"));

			int i = 1;

			for (var e : queryDup.asMap().entrySet())
			{
				dup.print(String.format("## %s\n", i++));
				e.getValue().forEach(dup::println);
				dup.println();
			}
			dup.close();
			TheDemo.measure("reformulations", "unique", queryDup.asMap().size());
		}
		else
		{
			q.forEach(t -> {
				queries.println(t);
				natives.println(access.treeToQNative(t));
				nb[0]++;
			});
		}
		queries.close();
		natives.close();
		TheDemo.measure("reformulations", "nb", nb[0]);
	}
}

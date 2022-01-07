package insomnia.demo.command;

import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

import insomnia.data.ITree;
import insomnia.demo.TheDemo;
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
import insomnia.rule.IRule;

final class ComGenerate implements ICommand
{
	private enum MyOptions
	{
		Output(Option.builder().longOpt("generate.output").desc("Output URIs for display").build()) //
		, DisplayRefs(Option.builder().longOpt("generate.display.refs").desc("(bool) Display the reformulations").build()) //
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
		return "generate";
	}

	@Override
	public String getDescription()
	{
		return "Generate all query reformulations";
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
		var reformulations = getReformulations(config);
		var summary        = Summary.get(config);

		Stream<ITree<Object, KVLabel>> st;

		if (summary.isEmpty())
			st = Trees.treesFromAutomatonStream(reformulations, 10);
		else
		{
			summary.consider(reformulations);
			st = summary.generateTrees();
		}
		return st;
	}

	private Consumer<ITree<Object, KVLabel>> generateAction(int[] nb, boolean displayRefs)
	{
		if (displayRefs)
			return t -> {
				out.println(t);
				nb[0]++;
			};
		else
			return t -> {
				nb[0]++;
			};
	}

	// ==========================================================================

	private PrintStream out;

	public void execute(Configuration config) throws Exception
	{
		int nb[] = new int[] { 0 };

		out = new PrintStream(InputData.getOutput(List.of(config.getString(MyOptions.Output.opt.getLongOpt()).split(","))), true);
		{
			var q = queries(config);

			var displayRefs = config.getBoolean(MyOptions.DisplayRefs.opt.getLongOpt(), false);
			q.forEach(generateAction(nb, displayRefs));
		}
		TheDemo.measure("reformulations", "nb", nb[0]);
	}

}

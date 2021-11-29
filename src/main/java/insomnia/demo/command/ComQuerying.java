package insomnia.demo.command;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheDemo;
import insomnia.demo.TheDemo.MEASURES;
import insomnia.demo.data.DataAccesses;
import insomnia.demo.data.IDataAccess.UFunctionTransform;
import insomnia.demo.input.InputData;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.cpu.CPUTimeBenchmark;

final class ComQuerying implements ICommand
{
	private enum MyOptions
	{
		Output(Option.builder().longOpt("querying.output").desc("Output URIs for display").build()) //
		, DisplayNb(Option.builder().longOpt("querying.display.nb").desc("Display the number of answers").build()) //
		, DisplayAnswers(Option.builder().longOpt("querying.display.answers").desc("Display the answers").build()) //
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

	private PrintStream out;

	public void execute(Configuration config) throws Exception
	{
		out = new PrintStream(InputData.getOutput(List.of(config.getString(MyOptions.Output.opt.getLongOpt()).split(","))), true);

		try
		{
			UFunctionTransform<Object, KVLabel> userWrap;

			var qtrans = TheDemo.measure(MEASURES.QTRANSFORM);

			userWrap = f -> o -> {
				qtrans.startChrono();
				var ret = f.apply(o);
				qtrans.stopChrono();
				return ret;
			};

			var dataAccess   = DataAccesses.getDataAccess(config);
			var resultStream = dataAccess.execute(ComGenerate.queries(config), userWrap);

			int count[] = new int[1];

			var qmes    = TheDemo.measure(MEASURES.QUERYING);
			var qstream = TheDemo.measure(MEASURES.QSTREAM);

			{
				var displayAnswers = config.getBoolean(MyOptions.DisplayAnswers.opt.getLongOpt(), false);
				CPUTimeBenchmark.startChrono(qmes, qstream);

				if (displayAnswers)
					resultStream.forEach(r -> {
						qstream.stopChrono();
						count[0]++;
						out.println(r);
						qstream.startChrono();
					});
				else
					resultStream.forEach(r -> {
						qstream.stopChrono();
						count[0]++;
						qstream.startChrono();
					});
				CPUTimeBenchmark.stopChrono(qmes, qstream);
			}

			var qprocess = TheDemo.measure(MEASURES.QPROCESS);
			qprocess.copy(qmes);
			qprocess.minus(qstream);

			if (config.getBoolean(MyOptions.DisplayNb.opt.getLongOpt(), false))
				out.printf("nb: %s\n", count[0]);
		}
		catch (URISyntaxException e)
		{
			throw new IllegalArgumentException(e);
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

}

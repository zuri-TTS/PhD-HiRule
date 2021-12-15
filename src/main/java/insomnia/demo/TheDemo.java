package insomnia.demo;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLStreamHandler;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;

import insomnia.demo.command.ICommand;
import insomnia.demo.command.TheCommands;
import insomnia.demo.help.URLStreamHandlerFactories;
import insomnia.demo.help.URLStreamHandlers;
import insomnia.demo.input.InputData;
import insomnia.lib.cpu.CPUTimeBenchmark;
import insomnia.lib.help.HelpFunctions;
import insomnia.lib.net.StdUrlStreamHandler;

public final class TheDemo
{
	private static final String cmdLine = "tfdemo";

	private static Measures measures = new Measures();

	static
	{
		setup();
	}

	private static void setup()
	{
		URL.setURLStreamHandlerFactory(URLStreamHandlerFactories.of(Map.<String, URLStreamHandler>of( //
			"ressource", URLStreamHandlers.from(HelpFunctions.unchecked(u -> {
				var path = u.getPath().substring(1); // Avoid the '/' first char
				return TheDemo.class.getClassLoader().getResource(path).openConnection();
			})) //
			, "std", URLStreamHandlers.from(HelpFunctions.unchecked(u -> {
				var std = u.getHost();

				switch (std)
				{
				case "out":
					return StdUrlStreamHandler.stdout(u);
				case "in":
					return StdUrlStreamHandler.stdin(u);
				default:
					throw new IllegalArgumentException(String.format("Protocol std: unknown host `%s`", std));
				}
			})))));
	}

	public static Measures getMeasures()
	{
		return measures;
	}

	public static CPUTimeBenchmark measure(String name)
	{
		return measures.getTime("measures", name);
	}

	public static CPUTimeBenchmark measure(String group, String name, CPUTimeBenchmark set)
	{
		measures.set(group, name, set);
		return set;
	}

	public static void measure(String group, String name, int set)
	{
		measures.set(group, name, set);
	}

	public static CPUTimeBenchmark measure(String name, CPUTimeBenchmark set)
	{
		measures.set("measures", name, set);
		return set;
	}

	public static Measures measures()
	{
		return measures;
	}

	public static PrintStream out()
	{
		return System.out;
	}

	public static void main(String[] args) throws IOException, ParseException
	{
		var    commands = TheCommands.commands();
		var    config   = TheConfiguration.getDefault();
		String cmd;

		if (args.length == 0)
		{
			System.out.println(String.format("Usage: %s command [options...]\n\t", cmdLine));
			cmd = config.getString("command.default");
		}
		else
			cmd = args[0];

		ICommand theCommand;
		{
			if (!commands.containsKey(cmd))
			{
				System.err.println(String.format("Unknow command `%s`", cmd));
				return;
			}
			theCommand = commands.get(cmd);

			args = ArrayUtils.subarray(args, 1, args.length);
		}

		try
		{
			var options = theCommand.getOptions();
			options.addOption(TheOptions.OneOption.Config.getOption());

			var cli = new DefaultParser().parse(options, args);
			// Load config files
			{
				var configOpt = TheOptions.OneOption.Config.getOption().getOpt();

				if (cli.hasOption(configOpt))
				{
					var cliConfigs = cli.getOptionValues(configOpt);
					var configs    = new ArrayList<Configuration>(cliConfigs.length + 1);
					configs.add(config);
					List.of(cliConfigs).forEach(c -> configs.add(TheConfiguration.getUnsafe(c)));
					Collections.reverse(configs);
					config = TheConfiguration.union(configs);
				}
			}
			var mes = measure("command");

			config.addProperty("#cli", cli);
			mes.startChrono();
			theCommand.execute(config);
			mes.stopChrono();

			var outMeasures = new PrintStream(InputData.getOutput(List.of(config.getString("output.measures", "std://out").split(","))), true);

			measures.print(outMeasures);
		}
		catch (Throwable e)
		{
			System.err.println(String.format("Exception raised during the command `%s`: %s", //
				theCommand.getName(), e.getMessage()));
			e.printStackTrace(System.err);
		}
	}
}

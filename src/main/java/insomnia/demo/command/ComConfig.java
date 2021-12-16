package insomnia.demo.command;

import java.io.PrintStream;

import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheDemo;
import insomnia.lib.help.HelpStream;

final class ComConfig implements ICommand
{
	@Override
	public String getName()
	{
		return "config";
	}

	@Override
	public String getDescription()
	{
		return "Show the actual configuration";
	}

	public static void print(Configuration config, PrintStream printer, boolean closePrinter)
	{
		HelpStream.toStream(config.getKeys()) //
			.sorted() //
			.forEach(k -> printer.printf("%s: %s\n", k, config.get(Object.class, k)));

		if (closePrinter)
			printer.close();
	}

	@Override
	public void execute(Configuration config)
	{
		print(config, TheDemo.out(), false);
	}
}

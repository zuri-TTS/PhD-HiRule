package insomnia.demo.command;

import java.io.PrintStream;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;

import insomnia.demo.TheDemo;

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
		ConfigurationUtils.dump(config, printer);

		if (closePrinter)
			printer.close();
	}

	@Override
	public void execute(Configuration config)
	{
		print(config, TheDemo.out(), false);
	}
}

package insomnia.demo.command;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration2.Configuration;

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

	@SuppressWarnings("unchecked")
	public static void print(Configuration config, PrintStream printer, boolean closePrinter)
	{
		for (final Iterator<String> keys = config.getKeys(); keys.hasNext();)
		{
			final String key   = keys.next();
			final Object value = config.getProperty(key);

			if (value instanceof List<?>)

				for (var item : ((List<Object>) value))
				{
					printer.print(key);
					printer.print("=");
					printer.println(item);
				}
			else
			{
				printer.print(key);
				printer.print("=");
				printer.println(value);
			}
		}

		printer.flush();

		if (closePrinter)
			printer.close();
	}

	@Override
	public void execute(Configuration config)
	{
		print(config, TheDemo.out(), false);
	}
}

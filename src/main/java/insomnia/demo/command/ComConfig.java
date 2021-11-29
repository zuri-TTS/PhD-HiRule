package insomnia.demo.command;

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

	@Override
	public void execute(Configuration config)
	{
		HelpStream.toStream(config.getKeys()) //
			.sorted() //
			.forEach(k -> TheDemo.out().printf("%s: %s\n", k, config.get(Object.class, k)));
	}

}

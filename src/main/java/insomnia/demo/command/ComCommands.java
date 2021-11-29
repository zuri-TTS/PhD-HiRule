package insomnia.demo.command;

import org.apache.commons.configuration2.Configuration;

final class ComCommands implements ICommand
{
	@Override
	public String getName()
	{
		return "commands";
	}

	@Override
	public String getDescription()
	{
		return "Display all available commands";
	}

	@Override
	public void execute(Configuration config)
	{
		TheCommands.commands().values().stream().sorted((a, b) -> a.getName().compareTo(b.getName())).forEach(cmd -> {
			System.out.println(String.format("%s: %s", cmd.getName(), cmd.getDescription()));
		});
	}

}

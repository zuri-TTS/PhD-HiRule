
package insomnia.demo.command;

import java.util.Map;

public final class TheCommands
{
	private TheCommands()
	{
		throw new AssertionError();
	}

	// ==========================================================================

	private static enum CommandsFactory
	{
		COMMANDS;

		Map<String, ICommand> i = Map.ofEntries( //
			new ComCommands().getEntry() //
			, new ComGenerate().getEntry() //
			, new ComSummarize().getEntry() //
			, new ComConfig().getEntry() //
			, new ComDbInfos().getEntry() //
			, new ComQuerying().getEntry() //
			, new ComHelp().getEntry() //
			, new ComPartition().getEntry() //
		);
	}

	public static Map<String, ICommand> commands()
	{
		return CommandsFactory.COMMANDS.i;
	}

	public static ICommand interpret(String command)
	{
		return commands().get(command);
	}
}

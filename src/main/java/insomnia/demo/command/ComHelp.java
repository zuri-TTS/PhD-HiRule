package insomnia.demo.command;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheDemo;

final class ComHelp implements ICommand
{
	@Override
	public String getName()
	{
		return "help";
	}

	@Override
	public String getDescription()
	{
		return "Display the help of a command";
	}

	@Override
	public void execute(Configuration config)
	{
		CommandLine cli  = config.get(CommandLine.class, "#cli");
		var         args = cli.getArgList();

		if (args.size() == 0)
		{
			TheDemo.out().println("help command");
			TheDemo.out().println(getDescription());
			return;
		}
		var cmd = TheCommands.interpret(args.get(0));

		if (null == cmd)
			throw new IllegalArgumentException(String.format("Unknowed command `%s`", args.get(0)));

		var formatter = new HelpFormatter();
		var header    = cmd.getDescription();
		header += "\n Warning: these are property to set in the configuration input file, not real cli arguments";
		formatter.printHelp(new PrintWriter(TheDemo.out(), true), 80, cmd.getName(), header, cmd.getConfigProperties(), 2, 2, "");
	}

}

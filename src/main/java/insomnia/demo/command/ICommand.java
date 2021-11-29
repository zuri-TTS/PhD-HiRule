package insomnia.demo.command;

import java.util.AbstractMap;
import java.util.Map;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;

public interface ICommand
{
	default Options getOptions()
	{
		return new Options();
	}

	default Options getConfigProperties()
	{
		return new Options();
	}

	String getName();

	String getDescription();

	void execute(Configuration config) throws Exception;

	// ========================================================================

	default Map.Entry<String, ICommand> getEntry()
	{
		return new AbstractMap.SimpleEntry<>(getName(), this);
	}

	default void displayHelp()
	{
		new HelpFormatter().printHelp(getName(), getDescription(), getOptions(), "");
	}
}

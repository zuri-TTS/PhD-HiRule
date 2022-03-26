package insomnia.demo.command;

import java.io.PrintWriter;
import java.net.URISyntaxException;

import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheDemo;
import insomnia.demo.data.DataAccesses;

final class ComDbInfos implements ICommand
{
	@Override
	public String getName()
	{
		return "dbinfos";
	}

	@Override
	public String getDescription()
	{
		return "Display some database informations";
	}

	// ==========================================================================

	public void execute(Configuration config) throws Exception
	{
		try
		{
			var dataAccess = DataAccesses.getDataAccess(config, TheDemo.measures());
			dataAccess.writeInfos(new PrintWriter(TheDemo.out(), true));
		}
		catch (URISyntaxException e)
		{
			throw new IllegalArgumentException(e);
		}
	}
}

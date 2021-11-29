package insomnia.demo;

import java.util.EnumSet;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public final class TheOptions
{
	private TheOptions()
	{
		throw new AssertionError();
	}

	// ==========================================================================

	public enum OneOption
	{
		Config(Option.builder("c").longOpt("config").hasArgs().desc("Some URLs to config files to load").hasArgs().build());
		;

		private Option opt;

		private OneOption(Option opt)
		{
			this.opt = opt;
		}

		public Option getOption()
		{
			return opt;
		}
	}

	// ==========================================================================

	public static Options getOptions(EnumSet<OneOption> options)
	{
		var ret = new Options();

		for (var e : options)
			ret.addOption(e.getOption());

		return ret;
	}

	public static Options getOptions()
	{
		return getOptions(EnumSet.allOf(OneOption.class));
	}
}

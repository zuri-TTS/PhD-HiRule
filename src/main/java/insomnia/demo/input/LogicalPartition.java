package insomnia.demo.input;

import insomnia.data.IPath;
import insomnia.implem.data.Paths;
import insomnia.implem.kv.KV;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.codec.IDecoder;
import insomnia.lib.numeric.Interval;
import insomnia.lib.numeric.MultiInterval;

public final class LogicalPartition
{
	private String name;

	private IPath<Object, KVLabel> prefix;

	private MultiInterval interval;

	private LogicalPartition()
	{
		name     = "";
		prefix   = Paths.empty();
		interval = MultiInterval.nullValue();
	}

	private LogicalPartition(String name, IPath<Object, KVLabel> prefix)
	{
		this.name   = name;
		this.prefix = prefix;
		interval    = MultiInterval.nullValue();
	}

	private static enum Factory
	{
		NULL;

		LogicalPartition p = new LogicalPartition();
	}

	public static LogicalPartition nullValue()
	{
		return Factory.NULL.p;
	}

	public boolean isNull()
	{
		return this == nullValue();
	}

	public static IDecoder<LogicalPartition> decoder()
	{
		return IDecoder.from(s -> {
			var split = s.split("[\\s,;]+");

			if (split.length < 2)
				throw new IllegalArgumentException(String.format("LogicalPartition representation must have at least 2 elements; has '%s'", s));

			var name   = split[0];
			var prefix = (IPath<Object, KVLabel>) KV.treeFromString(split[1]);

			var ret = new LogicalPartition(name, prefix);

			if (split.length > 2)
				ret.interval = MultiInterval.of(Interval.decoder().decode(split[2]));

			return ret;
		});
	}

	public String getName()
	{
		return name;
	}

	public IPath<Object, KVLabel> getPrefix()
	{
		return prefix;
	}

	public MultiInterval getInterval()
	{
		return interval;
	}
}

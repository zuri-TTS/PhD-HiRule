package insomnia.demo.input;

import java.util.Optional;

import insomnia.data.IPath;
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

	}

	public static IDecoder<LogicalPartition> decoder()
	{
		return IDecoder.from(s -> {
			var split = s.split("[\\s,;]+");

			if (split.length < 2)
				throw new IllegalArgumentException(String.format("LogicalPartition representation must have at least 2 elements; has '%s'", s));

			var ret = new LogicalPartition();
			ret.name   = split[0];
			ret.prefix = (IPath<Object, KVLabel>) KV.treeFromString(split[1]);

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

	public Optional<MultiInterval> getInterval()
	{
		return Optional.ofNullable(interval);
	}
}

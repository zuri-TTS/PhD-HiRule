package insomnia.demo;

import java.util.Collection;

import org.apache.commons.configuration2.Configuration;

import insomnia.data.ITree;
import insomnia.implem.kv.data.KVLabel;
import insomnia.rule.IRule;
import insomnia.summary.ISummary;

public interface IDemoData
{
	ITree<Object, KVLabel> getQuery();

	Collection<IRule<Object, KVLabel>> getRules();

	ISummary<Object, KVLabel> getSummary();

	Configuration getConfiguration();

	public static IDemoData of( //
		ITree<Object, KVLabel> query //
		, Collection<IRule<Object, KVLabel>> rules //
		, ISummary<Object, KVLabel> summary //
		, Configuration config //
	)
	{
		return new IDemoData()
		{
			@Override
			public Collection<IRule<Object, KVLabel>> getRules()
			{
				return rules;
			}

			@Override
			public ITree<Object, KVLabel> getQuery()
			{
				return query;
			}

			@Override
			public ISummary<Object, KVLabel> getSummary()
			{
				return summary;
			}

			@Override
			public Configuration getConfiguration()
			{
				return config;
			}
		};
	}
}

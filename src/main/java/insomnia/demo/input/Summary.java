package insomnia.demo.input;

import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheConfiguration;
import insomnia.demo.input.InputData.Filters;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.LabelSummaryReader;
import insomnia.summary.ISummary;

public final class Summary
{
	private Summary()
	{
		throw new AssertionError();
	}

	public static ISummary<Object, KVLabel> get(Configuration config) throws IOException, ParseException
	{
		return get(config.getString(TheConfiguration.OneProperty.Summary.getPropertyName()));
	}

	public static ISummary<Object, KVLabel> get(String uri) throws IOException, ParseException
	{
		if (uri.isEmpty())
			return LabelSummary.create();

		var optPath = InputData.getPath(uri);

		if (optPath.isEmpty())
			throw new IllegalArgumentException(String.format("The summary file: `%s` does not exists", uri));

		var data = InputData.filters(InputData.getLinesOf(optPath.get()), Filters.NO_BLANK);
		return new LabelSummaryReader<Object, KVLabel>().setReadLabel(KVLabels::parseIllegal).read(data.iterator());
	}
}

package insomnia.demo.input;

import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;

import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheConfiguration;
import insomnia.demo.input.InputData.Filters;
import insomnia.implem.kv.KV;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.LabelSummaryReader;
import insomnia.implem.summary.LabelTypeSummaryReader;
import insomnia.implem.summary.PathSummary;
import insomnia.lib.codec.IDecoder;
import insomnia.summary.ISummary;

public final class Summary
{
	public enum Type
	{
		KEY, KEY_TYPE, PATH;
	}

	private Summary()
	{
		throw new AssertionError();
	}

	public static ISummary<Object, KVLabel> get(Configuration config) throws IOException, ParseException
	{
		Type type;
		switch (config.getString("summary.type"))
		{
		case "path":
			type = Type.PATH;
			break;
		case "key-type":
			type = Type.KEY_TYPE;
			break;
		case "key":
			type = Type.KEY;
			break;
		default:
			throw new IllegalArgumentException(String.format("Invalid summary.type: %s", config.getString("summary.type")));
		}
		return get(config.getString(TheConfiguration.OneProperty.Summary.getPropertyName()), type);
	}

	private static ISummary<Object, KVLabel> get(String uri, Type type) throws IOException, ParseException
	{
		if (uri.isEmpty())
			return LabelSummary.create();

		var optPath = InputData.getPath(uri);

		if (optPath.isEmpty())
			throw new IllegalArgumentException(String.format("The summary file: `%s` does not exists", uri));

		switch (type)
		{
		case PATH:
		{
			var s = PathSummary.<Object, KVLabel>create();
			s.addTree(KV.treeFromString(Files.readString(optPath.get())));
			return s;
		}
		case KEY:
		{
			var data = InputData.filters(InputData.getLinesOf(optPath.get()), Filters.NO_BLANK);
			return new LabelSummaryReader<Object, KVLabel>().setReadLabel(IDecoder.from(KVLabels::parseIllegal)).read(data.iterator());
		}
		case KEY_TYPE:
		{
			var data = InputData.filters(InputData.getLinesOf(optPath.get()), Filters.NO_BLANK);
			return new LabelTypeSummaryReader<Object, KVLabel>().setReadLabel(IDecoder.from(KVLabels::parseIllegal)).read(data.iterator());
		}
		default:
			throw new AssertionError();
		}
	}
}

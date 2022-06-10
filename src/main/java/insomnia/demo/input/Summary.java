package insomnia.demo.input;

import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.EnumSet;
import java.util.function.Function;

import org.apache.commons.configuration2.Configuration;

import insomnia.data.ITree;
import insomnia.demo.TheConfiguration;
import insomnia.demo.input.InputData.Filters;
import insomnia.implem.data.TreeFilters;
import insomnia.implem.data.TreeFilters.NodeInfos;
import insomnia.implem.kv.KV;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.LabelSummaryReader;
import insomnia.implem.summary.PathSummary;
import insomnia.lib.codec.IDecoder;
import insomnia.lib.help.HelpFunctions;
import insomnia.summary.ISummary;

public final class Summary
{
	public enum Type
	{
		LABEL, PATH;
	}

	private Summary()
	{
		throw new AssertionError();
	}

	public static Type parseType(String stype)
	{
		Type type;
		switch (stype.toLowerCase())
		{
		case "path":
			type = Type.PATH;
			break;
		case "label":
			type = Type.LABEL;
			break;
		default:
			throw new IllegalArgumentException(String.format("Invalid summary.type: %s", stype));
		}
		return type;
	}

	public static ISummary<Object, KVLabel> get(Configuration config) throws IOException, ParseException
	{
		var uri = config.getString(TheConfiguration.OneProperty.Summary.getPropertyName());
		return get(config, uri);
	}

	public static ISummary<Object, KVLabel> get(Configuration config, String uri) throws IOException, ParseException
	{
		if (uri.isEmpty())
			return LabelSummary.create();

		var filters = EnumSet.noneOf(TreeFilters.Filters.class);

		if (config.getBoolean(TheConfiguration.OneProperty.SummaryFilterTypes.getPropertyName(), true))
			filters.add(TreeFilters.Filters.TYPE);

		int stringValuePrefixSize = config.getInt(TheConfiguration.OneProperty.SummaryFilterStringValuePrefix.getPropertyName(), 0);

		if (stringValuePrefixSize != 0)
			filters.add(TreeFilters.Filters.STRING_VALUE_PREFIX);

		return get(uri, parseType(config.getString("summary.type")), filters);
	}

	public static ISummary<Object, KVLabel> get(String uri, Type type, EnumSet<TreeFilters.Filters> filters) throws IOException, ParseException
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
			var stree   = KV.treeFromString(Files.readString(optPath.get()));
			var decoder = NodeInfos.decoder();
			var tree    = ITree.update(stree, HelpFunctions.unchecked(v -> decoder.decode((String) v)), Function.identity());
			var s       = PathSummary.create(tree);
			s.filterTypes().addAll(filters);
			return s;
		}
		case LABEL:
		{
			var data = InputData.filters(InputData.getLinesOf(optPath.get()), Filters.NO_BLANK);
			var s    = (new LabelSummaryReader<Object, KVLabel>().setReadLabel(IDecoder.from(KVLabels::parseIllegal)).read(data.iterator()));
			s.filterTypes().addAll(filters);
			return s;
		}
		default:
			throw new AssertionError();
		}
	}
}

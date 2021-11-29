package insomnia.demo.input;

import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration2.Configuration;

import insomnia.demo.TheConfiguration;
import insomnia.demo.input.InputData.Filters;
import insomnia.implem.kv.KV;
import insomnia.implem.kv.data.KVLabel;
import insomnia.lib.help.HelpFunctions;
import insomnia.rule.IRule;

public final class Rules
{
	private Rules()
	{
		throw new AssertionError();
	}

	private static IRule<Object, KVLabel> parseARule(String line) throws ParseException
	{
		String[] elements = line.split(" *[-=]+> *");
		return KV.ruleFromString(elements[0].trim(), elements[1].trim());
	}

	public static List<IRule<Object, KVLabel>> get(Configuration config) throws IOException, ParseException
	{
		return get(config.getString(TheConfiguration.OneProperty.Rules.getPropertyName()));
	}

	private static Stream<String> getLinesOf(Path p)
	{
		return HelpFunctions.<Path, Stream<String>>unchecked(InputData::getLinesOf).apply(p);
	}

	public static List<IRule<Object, KVLabel>> get(String uri) throws IOException, ParseException
	{
		if (uri.isBlank())
			return List.of();

		var filesOpt = InputData.getDirFiles(uri);

		if (filesOpt.isEmpty())
			throw new IllegalArgumentException(String.format("URI `%s` is invalid", uri));

		var data = filesOpt.get().flatMap(p -> InputData.filters(getLinesOf(p), Filters.NO_BLANK, Filters.NO_COMMENT));
		return data.map(HelpFunctions.unchecked(Rules::parseARule)).collect(Collectors.toList());
	}
}

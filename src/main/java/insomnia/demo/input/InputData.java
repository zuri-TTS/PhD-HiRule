package insomnia.demo.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;

import insomnia.lib.help.HelpFunctions;
import insomnia.lib.help.HelpURI;
import insomnia.lib.io.SequenceOutputStream;

public final class InputData
{
	private InputData()
	{
		throw new AssertionError();
	}

	public static Optional<URI> getURI(String uri)
	{
		try
		{
			return Optional.of(new URI(HelpURI.encodeURI(uri)));
		}
		catch (URISyntaxException e)
		{
			return Optional.empty();
		}
	}

	public static Optional<Path> getPath(String path)
	{
		var uri = getURI(path);

		if (uri.isEmpty())
			return Optional.of(Paths.get(path));

		try
		{
			return Optional.of(Paths.get(uri.get()));
		}
		catch (InvalidPathException e)
		{
			return Optional.empty();
		}
	}

	/**
	 * Return all files in the path directory.
	 * If path is a file then return it.
	 */
	public static Optional<Stream<Path>> getDirFiles(String path) throws IOException
	{
		var pathOpt = getPath(path);

		if (pathOpt.isEmpty() || !Files.exists(pathOpt.get()))
			return Optional.empty();

		var thePath = pathOpt.get();

		if (Files.isDirectory(thePath))
			return Optional.of(Files.list(thePath));

		return Optional.of(Stream.of(thePath));
	}

	public static InputStream tryOpenPath(Path path, OpenOption... options) throws IOException
	{
		return Files.newInputStream(path, options);
	}

	public static InputStream tryOpenStream(String uri) throws IOException
	{
		var opt = getURI(uri);

		if (opt.isEmpty())
			return tryOpenPath(Paths.get(uri));

		return opt.get().toURL().openStream();
	}

	public static OutputStream fakeOpenOutputPath(Path path, OpenOption... options)
	{
		try
		{
			return Files.newOutputStream(path, options);
		}
		catch (IOException e)
		{
			return OutputStream.nullOutputStream();
		}
	}

	public static OutputStream tryOpenOutputPath(Path path, OpenOption... options) throws IOException
	{
		return Files.newOutputStream(path, options);
	}

	public static OutputStream tryOpenOutputStream(String uri) throws IOException
	{
		var opt = getURI(uri);

		if (opt.isEmpty())
			return tryOpenOutputPath(Paths.get(uri));

		return opt.get().toURL().openConnection().getOutputStream();
	}

	public static Stream<String> getLinesOf(String uri) throws IOException
	{
		return getLinesOf(tryOpenStream(uri));
	}

	public static Stream<String> getLinesOf(Path path) throws IOException
	{
		return Files.newBufferedReader(path).lines();
	}

	public static Stream<String> getLinesOf(InputStream in) throws IOException
	{
		return new BufferedReader(new InputStreamReader(in)).lines();
	}

	public static OutputStream getOutput(List<String> uris)
	{
		var streams = uris.stream().map(HelpFunctions.unchecked(InputData::tryOpenOutputStream)).collect(Collectors.toList());

		return IOUtils.buffer(new SequenceOutputStream(streams));
	}

	// ==========================================================================

	public static final String COMMENT_CHAR    = "#";
	public static final String MLCOMMENT_START = "#(";
	public static final String MLCOMMENT_END   = ")#";

	public enum Filters
	{
		NO_BLANK(() -> s -> !s.isBlank()) //
		, NO_COMMENT(() -> {
			boolean ml[] = new boolean[] { false };

			return s -> {

				if (ml[0])
				{
					if (s.stripTrailing().endsWith(MLCOMMENT_END))
						ml[0] = false;

					return false;
				}
				else
				{
					if (s.startsWith(MLCOMMENT_START))
					{
						ml[0] = true;
						return false;
					}
					else if (s.startsWith(COMMENT_CHAR))
						return false;

					return true;
				}
			};
		});

		private Supplier<Predicate<String>> test;

		Filters(Supplier<Predicate<String>> test)
		{
			this.test = test;
		}

		public Stream<String> filter(Stream<String> stream)
		{
			return stream.filter(test.get());
		}
	};

	public static Stream<String> filters(Stream<String> stream, Filters first, Filters... filters)
	{
		return filters(stream, EnumSet.of(first, filters));
	}

	public static Stream<String> filters(Stream<String> stream, EnumSet<Filters> filters)
	{
		for (var f : filters)
			stream = f.filter(stream);

		return stream;
	}
}

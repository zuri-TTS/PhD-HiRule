package insomnia.demo.help;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.function.Function;

public final class URLStreamHandlers
{
	private URLStreamHandlers()
	{
		throw new AssertionError();
	}

	public static URLStreamHandler from(Function<URL, URLConnection> f)
	{
		return new URLStreamHandler()
		{
			@Override
			protected URLConnection openConnection(URL url) throws IOException
			{
				return f.apply(url);
			}
		};
	}
}

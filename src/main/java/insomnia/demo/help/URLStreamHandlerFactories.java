package insomnia.demo.help;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Map;

public final class URLStreamHandlerFactories
{
	private URLStreamHandlerFactories()
	{
		throw new AssertionError();
	}

	public static URLStreamHandlerFactory of(Map<String, URLStreamHandler> protocolToHandler)
	{
		return new URLStreamHandlerFactory()
		{
			private Map<String, URLStreamHandler> _protocolToHandler = Map.copyOf(protocolToHandler);

			@Override
			public URLStreamHandler createURLStreamHandler(String protocol)
			{
				return _protocolToHandler.get(protocol);
			}
		};
	}
}

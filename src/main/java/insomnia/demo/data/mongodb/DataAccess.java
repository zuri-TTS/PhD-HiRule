package insomnia.demo.data.mongodb;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Triple;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.ConnectionString;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import insomnia.data.INode;
import insomnia.data.ITree;
import insomnia.demo.TheDemo;
import insomnia.demo.data.IDataAccess;
import insomnia.implem.data.Trees;
import insomnia.implem.data.creational.TreeBuilder;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.kv.data.KVValues;
import insomnia.lib.help.HelpStream;

public final class DataAccess implements IDataAccess<Object, KVLabel>
{
	private MongoClient      client;
	private ConnectionString connection;

	private MongoCollection<Document> collection;

	private int batchSize;

	private boolean checkTerminalLeaf;

	private DataAccess(URI uri, Configuration config)
	{
		connection = new ConnectionString(uri.toString());
		client     = MongoClients.create(connection);
		collection = client.getDatabase(connection.getDatabase()).getCollection(connection.getCollection());

		batchSize = config.getInt("mongodb.batchSize", 100);

		checkTerminalLeaf = config.getBoolean("mongodb.leaf.checkTerminal", true);
	}

	// ==========================================================================

	public static IDataAccess<Object, KVLabel> open(URI uri, Configuration config)
	{
		return new DataAccess(uri, config);
	}

	private static ITree<Object, KVLabel> doc2Tree(Document doc)
	{
		var tb = new TreeBuilder<Object, KVLabel>();
		bson2Tree(tb, doc.toBsonDocument());
		return Trees.create(tb);
	}

	@Override
	public ITree<Object, KVLabel> nativeToTree(Object nativeRecord)
	{
		return doc2Tree((Document) nativeRecord);
	}

	@Override
	public Object treeToQNative(ITree<Object, KVLabel> query)
	{
		return tree2Query(query);
	}

	private static void bson2Tree(TreeBuilder<Object, KVLabel> sb, BsonValue doc)
	{
		if (doc.isDocument())
		{
			for (var entry : doc.asDocument().entrySet())
			{
				var label = entry.getKey();
				var value = entry.getValue();

				sb.addChildDown(KVLabels.create(label));
				bson2Tree(sb, value);
			}

			if (sb.getRoot() != sb.getCurrentNode())
				sb.goUp();
		}
		else if (doc.isArray())
		{
			var label = sb.getParent(sb.getCurrentNode()).get().getLabel();
			sb.removeUp();

			for (var val : doc.asArray().getValues())
			{
				sb.addChildDown(label);
				bson2Tree(sb, val);
			}
		}
		else if (doc.isString())
		{
			sb.setValue(doc.asString().getValue());
			sb.goUp();
		}
		else if (doc.isNumber())
		{
			sb.setValue(Double.valueOf(doc.asNumber().doubleValue()));
			sb.goUp();
		}
		else if (doc.isNull())
			sb.goUp();
		else if (doc.isObjectId())
		{
			sb.setValue(doc.asObjectId().getValue().toString());
			sb.goUp();
		}
		else
			throw new IllegalArgumentException(String.format("Cannot handle %s value", doc));
	}

	private Bson tree2Query(ITree<Object, KVLabel> tree)
	{
		var q2native = TheDemo.measure("each.query.query2native");
		q2native.startChrono();
		var filter = tree2Query(tree, tree.getRoot());
		q2native.stopChrono();
		return filter;
	}

	private Bson tree2Query( //
		ITree<Object, KVLabel> tree, INode<Object, KVLabel> node //
	)
	{
		if (0 == tree.nbChildren(node))
		{
			var parent = tree.getParent(node);

			if (parent.isEmpty())
				return Filters.empty();

			return null;
		}
		else
		{
			var childs = new ArrayList<Bson>(tree.nbChildren(node));

			for (var c : tree.getChildren(node))
			{
				var  label    = c.getLabel().asString();
				var  value    = c.getChild().getValue();
				var  subQuery = tree2Query(tree, c.getChild());
				Bson filter;

				// No childs
				if (null == subQuery)
				{
					BsonValue bval;

					if (value == null || KVValues.interpretation().isAny(value))
					{
						if (c.getChild().isTerminal() && checkTerminalLeaf)
							filter = //
								Filters.and( //
									Filters.exists(label), //
									Filters.not( //
										Filters.elemMatch(label, //
											Filters.or( //
												Filters.type(label, BsonType.ARRAY), //
												Filters.type(label, BsonType.DOCUMENT) //
											) //
										) //
									)//
								) //
							;
						else
							filter = Filters.exists(label);
					}
					else
					{
						if (value instanceof String)
							bval = new BsonString((String) value);
						else if (value instanceof Number)
							bval = new BsonDouble(((Number) value).doubleValue());
						else
							throw new IllegalArgumentException(String.format("Can't handle value '%s'", value));

						filter = Filters.eq(label, bval);
					}
				}
				else
					filter = Filters.elemMatch(label, subQuery);

				childs.add(filter);
			}

			if (childs.size() == 1)
				return childs.get(0);

			return Filters.and(childs);
		}
	}

	@Override
	public Stream<ITree<Object, KVLabel>> all()
	{
		return HelpStream.toStream(collection.find()) //
			.map(DataAccess::doc2Tree);
	}

	private Stream<Object> wrapDocumentCursor(FindIterable<Document> cursor)
	{
		return HelpStream.<Object>toStreamDownCast(cursor);
	}

	@Override
	public void encodeNativeQuery(Object query, PrintStream printer)
	{
		Bson qnative = (Bson) query;
		printer.println(qnative.toBsonDocument().toJson());
	}

	@Override
	public Object decodeNativeQuery(String from)
	{
		return Document.parse(from);
	}

	// ==========================================================================

	@Override
	public Stream<Triple<ITree<Object, KVLabel>, Object, Stream<Object>>> executeEach(Stream<ITree<Object, KVLabel>> queries)
	{
		var firstEval = TheDemo.measure("query.eval.stream.create");

		return queries.map(q -> {
			var bsonq = tree2Query(q);
			firstEval.startChrono();
			var cursor = collection.find(bsonq);
			firstEval.stopChrono();
			return Triple.of(q, bsonq, wrapDocumentCursor(cursor));
		});
	}

	private Stream<Object> execute(List<ITree<Object, KVLabel>> queries)
	{
		return executeBson(IterableUtils.transformedIterable(queries, this::tree2Query));
	}

	@SuppressWarnings("unchecked")
	private Stream<Object> executeNative(List<? extends Object> queries)
	{
		return executeBson((List<Bson>) queries);
	}

	private Stream<Object> executeBson(Iterable<Bson> queries)
	{
		var firstEval = TheDemo.measure("query.eval.stream.create");

		var disjunction = Filters.or(queries);
		firstEval.startChrono();
		var cursor = collection.find(disjunction);
		firstEval.stopChrono();
		return wrapDocumentCursor(cursor);
	}

	// ==========================================================================

	private long[] nbQueries;

	@Override
	public Stream<Object> execute(Stream<ITree<Object, KVLabel>> queries)
	{
		nbQueries = new long[] { 0 };
		var batch = HelpStream.batch(queries, batchSize, nbQueries);

		return batch.flatMap(this::execute);
	}

	@Override
	public long getNbQueries()
	{
		return nbQueries[0];
	}

	@Override
	public Stream<Object> executeNatives(Stream<Object> nativeQueries)
	{
		nbQueries = new long[] { 0 };
		var batch = HelpStream.batch(nativeQueries, batchSize, nbQueries);

		return batch.flatMap(this::executeNative);
	}

	@Override
	public String getRecordId(Object record)
	{
		return ((Document) record).getObjectId("_id").toHexString();
	}

	@Override
	public void writeInfos(PrintWriter printer)
	{
		printer.println(collection.getNamespace().getFullName());
		printer.printf("nb docs: %d\n", collection.countDocuments());
	}
}

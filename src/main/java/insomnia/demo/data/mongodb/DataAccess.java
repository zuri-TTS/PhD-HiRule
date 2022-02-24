package insomnia.demo.data.mongodb;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Triple;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.ConnectionString;
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import insomnia.data.INode;
import insomnia.data.ITree;
import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.TheDemo.TheMeasures;
import insomnia.demo.data.IDataAccess;
import insomnia.demo.input.Summary;
import insomnia.implem.data.Trees;
import insomnia.implem.data.creational.TreeBuilder;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.kv.data.KVValues;
import insomnia.implem.summary.LabelTypeSummary;
import insomnia.lib.cpu.CPUTimeBenchmark;
import insomnia.lib.cpu.CPUTimeBenchmark.TIME;
import insomnia.lib.help.HelpStream;
import insomnia.summary.ISummary.NodeType;

public final class DataAccess implements IDataAccess<Object, KVLabel>
{
	private enum MyOptions
	{
		QUERY_BATCHSIZE(Option.builder().longOpt("query.batchSize").desc("(int) How many queries to send at once to MongoDB").build()), //
		DATA_BATCHSIZE(Option.builder().longOpt("data.batchSize").desc("(int) How many records MongoDB must batch").build()), //
		LEAF_CHECKTERMINAL(Option.builder().longOpt("leaf.checkTerminal").desc("(bool) If true, the native MongoDB query will have constraints to check if a terminal node in the query is a terminal node in the result").build()), //
		INHIBIT_BATCH_STREAM_TIME(Option.builder().longOpt("inhibitBatchStreamTime").desc("(bool) If true, does it best to not count the time passed in the result stream to construct batches of reformulations").build()), //
		Q2NATIVE_DOTS(Option.builder().longOpt("toNative.dots").desc("(bool) If true, simplify simple paths using the dot notation").build()), //
		Q2NATIVE_USE_SUMMARY(Option.builder().longOpt("toNative.useSummary").desc("(bool) If true, use the key-type summary types to build the query and choose beetween dots notation or $elemMatch").build()), //
		;

		Option opt;

		private MyOptions(Option o)
		{
			opt = o;
		}
	}

	public static Options getItsConfigProperties()
	{
		var ret = new Options();

		for (var opt : List.of(MyOptions.values()))
			ret.addOption(opt.opt);

		return ret;
	}

	// ==========================================================================

	private MongoClient      client;
	private ConnectionString connection;

	private MongoCollection<Document> collection;

	private int queryBatchSize, dataBatchSize;

	private boolean checkTerminalLeaf;

	private boolean inhibitBatchStreamTime;

	private boolean q2NativeDots;

	private CPUTimeBenchmark inhibitTime;

	private Function<KVLabel, EnumSet<NodeType>> labelType;

	private DataAccess(URI uri, Configuration config)
	{
		connection = new ConnectionString(uri.toString());

		client     = MongoClients.create(connection);
		collection = client.getDatabase(connection.getDatabase()).getCollection(connection.getCollection());

		queryBatchSize         = config.getInt(MyOptions.QUERY_BATCHSIZE.opt.getLongOpt(), 100);
		dataBatchSize          = config.getInt(MyOptions.DATA_BATCHSIZE.opt.getLongOpt(), 0);
		checkTerminalLeaf      = config.getBoolean(MyOptions.LEAF_CHECKTERMINAL.opt.getLongOpt(), true);
		inhibitBatchStreamTime = config.getBoolean(MyOptions.INHIBIT_BATCH_STREAM_TIME.opt.getLongOpt(), true);
		q2NativeDots           = config.getBoolean(MyOptions.Q2NATIVE_DOTS.opt.getLongOpt(), false);

		if (inhibitBatchStreamTime)
			inhibitTime = TheDemo.measure(TheMeasures.QEVAL_STREAM_INHIB);

		if (config.getBoolean(MyOptions.Q2NATIVE_USE_SUMMARY.opt.getLongOpt(), false))
		{
			if (q2NativeDots)
				throw new IllegalArgumentException(String.format("%s cannot be set with %s", //
					MyOptions.Q2NATIVE_DOTS.opt.getLongOpt(), //
					MyOptions.Q2NATIVE_USE_SUMMARY.opt.getLongOpt()//
				));

			try
			{
				var summary = Summary.get(config);

				if (!(summary instanceof LabelTypeSummary<?, ?>))
					throw new IllegalArgumentException(String.format("Can only use %s with key-type summary: %s given", //
						MyOptions.Q2NATIVE_USE_SUMMARY.opt.getLongOpt(), //
						config.getString(TheConfiguration.OneProperty.Summary.getPropertyName()) //
					));
				labelType = ((LabelTypeSummary<Object, KVLabel>) summary)::getNodeType;
			}
			catch (IOException | ParseException e)
			{
				throw new Error(e);
			}
		}
		else
		{
			var type = EnumSet.of(NodeType.ARRAY);
			labelType = l -> type;
		}
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

			var arr = doc.asArray();

			for (var val : arr.getValues())
			{
				sb.addChildDown(label);
				bson2Tree(sb, val);
			}

			// Force value to be an array in the tree
			if (arr.size() == 1)
				sb.addChild(label, "");
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

	private static CPUTimeBenchmark q2native = TheDemo.measure(TheMeasures.QUERY_TO_NATIVE);

	private Bson tree2Query(ITree<Object, KVLabel> tree)
	{
		q2native.startChrono();
		BsonDocument filter = new BsonDocument();
		tree2Query(filter, tree, tree.getRoot());
		q2native.stopChrono();
		return filter;
	}

	private boolean isInt(double val)
	{
		return val == Math.ceil(val);
	}

	private void tree2Query(BsonDocument document, ITree<Object, KVLabel> tree, INode<Object, KVLabel> node)
	{
		var childs = tree.getChildren(node);

		for (var c : childs)
			tree2Query(document, new StringBuilder(c.getLabel().asString()), tree, c.getChild(), labelType.apply(c.getLabel()));
	}

	private static boolean isExists_op(BsonValue val)
	{
		if (!(val instanceof BsonDocument))
			return false;
		var doc = val.asDocument();

		return doc.size() == 1 && doc.get("$exists", BsonNull.VALUE).equals(BsonBoolean.TRUE);
	}

	private static boolean isAll_op(BsonValue val)
	{
		if (!(val instanceof BsonDocument))
			return false;
		var doc = val.asDocument();

		return doc.size() == 1 && doc.get("$all", BsonNull.VALUE) instanceof BsonArray;
	}

	private static boolean isScalar(BsonValue val)
	{
		return val instanceof BsonString //
			|| val instanceof BsonInt32 //
			|| val instanceof BsonDouble;
	}

	private void tree2Query( //
		BsonDocument document, StringBuilder labelBuilder, //
		ITree<Object, KVLabel> tree, INode<Object, KVLabel> node, //
		EnumSet<NodeType> type //
	)
	{
		var childs   = tree.getChildren(node);
		var nbChilds = childs.size();

		if (0 == nbChilds)
		{
			BsonValue bsonValue;
			var       value = node.getValue();

			if (value == null || KVValues.interpretation().isAny(value))
			{
				if (node.isTerminal() && checkTerminalLeaf)
				{
					bsonValue = new BsonDocument() //
						.append("$exists", BsonBoolean.TRUE) //
						.append("$not", new BsonDocument("$type", new BsonArray( //
							List.of(new BsonInt32(BsonType.ARRAY.getValue()), new BsonInt32(BsonType.DOCUMENT.getValue())) //
						)) //
						);
				}
				else
					bsonValue = new BsonDocument("$exists", BsonBoolean.TRUE);
			}
			else if (value instanceof String)
				bsonValue = new BsonString((String) value);
			else if (value instanceof Number)
			{
				var d = ((Number) value).doubleValue();

				if (isInt(d))
					bsonValue = new BsonInt32((int) d);
				else
					bsonValue = new BsonDouble(d);
			}
			else
				throw new IllegalArgumentException(String.format("Can't handle value '%s'", value));

			var label    = labelBuilder.toString();
			var existing = document.get(label);

			if (null != existing)
			{
				var e = isExists_op(existing);
				var b = isExists_op(bsonValue);

				if (b)
					;
				else if (e)
					document.append(label, bsonValue);
				else
				{
					boolean isAll_op = isAll_op(existing);

					if (!(isAll_op || isScalar(existing)) || !isScalar(bsonValue))
						throw new IllegalArgumentException(String.format("Label '%s' already set (=%s), wanna add %s", label, existing.toString(), bsonValue.toString()));

					if (isAll_op)
						existing.asDocument().get("$all").asArray().add(bsonValue);
					else
					{
						bsonValue = new BsonDocument("$all", new BsonArray(List.of(existing, bsonValue)));
						document.append(label, bsonValue);
					}
				}
			}
			else
				document.append(label, bsonValue);
		}
		else if (q2NativeDots && nbChilds == 1)
		{
			KVLabel label = null;

			while (nbChilds == 1)
			{
				var c = childs.get(0);
				label = c.getLabel();
				labelBuilder.append('.').append(label.asString());
				node     = c.getChild();
				childs   = tree.getChildren(node);
				nbChilds = childs.size();
			}
			tree2Query(document, labelBuilder, tree, node, labelType.apply(label));
		}
		else if (nbChilds == 1 && !type.contains(NodeType.ARRAY))
		{
			EnumSet<NodeType> ltype = null;

			while (nbChilds == 1)
			{
				var c     = childs.get(0);
				var label = c.getLabel();
				ltype = labelType.apply(label);
				labelBuilder.append('.').append(c.getLabel().asString());
				node = c.getChild();

				if (ltype.contains(NodeType.ARRAY))
					break;

				childs   = tree.getChildren(node);
				nbChilds = childs.size();
			}
			tree2Query(document, labelBuilder, tree, node, ltype);
		}
		else
		{
			int minLength;

			if (type.containsAll(List.of(NodeType.ARRAY, NodeType.OBJECT)))
				throw new Error(String.format("[mongodb] a label must have only one type: '%s' is %s", labelBuilder.toString(), type.toString()));

			if (type.contains(NodeType.ARRAY))
			{
				BsonDocument eMatch = new BsonDocument();
				document.append(labelBuilder.toString(), new BsonDocument("$elemMatch", eMatch));
				document  = eMatch;
				minLength = 0;
			}
			else
			{
				labelBuilder.append(".");
				minLength = labelBuilder.length();
			}
			labelBuilder = new StringBuilder(labelBuilder);

			for (var c : childs)
			{
				var label = c.getLabel();

				labelBuilder.setLength(minLength);
				labelBuilder.append(label.asString());
				tree2Query(document, labelBuilder, tree, c.getChild(), labelType.apply(label));
			}
		}
	}

	@Override
	public long getNbDocuments()
	{
		return collection.estimatedDocumentCount();
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
		return queries.map(q -> {
			var bsonq  = tree2Query(q);
			var cursor = collection.find(bsonq);
			return Triple.of(q, bsonq, wrapDocumentCursor(cursor));
		});
	}

	@Override
	public boolean hasAnswer(ITree<Object, KVLabel> query)
	{
		var bsonq  = tree2Query(query);
		var cursor = collection.find(bsonq).limit(1);
		return null != cursor.first();
	}

	@SuppressWarnings("unchecked")
	private Stream<Object> executeNative(List<? extends Object> queries)
	{
		return executeBson((List<Bson>) queries);
	}

	private Stream<Object> explainBson(Iterable<Bson> queries)
	{
		var disjunction = Filters.or(queries);

		var stats = collection.find(disjunction).explain(ExplainVerbosity.EXECUTION_STATS);
		return Stream.of(stats);
	}

	private Stream<Object> executeBson(Iterable<Bson> queries)
	{
		var disjunction = Filters.or(queries);

		var cursor = collection.find(disjunction);

		if (dataBatchSize > 0)
			cursor.batchSize(dataBatchSize);

		return wrapDocumentCursor(cursor);
	}

	@Override
	public insomnia.demo.data.IDataAccess.explainStats explainStats(Object record)
	{
		var doc       = ((Document) record);
		var time      = doc.getEmbedded(List.of("executionStats"), Document.class).getInteger("executionTimeMillis");
		var nbAnswers = doc.getEmbedded(List.of("executionStats"), Document.class).getInteger("nReturned");

		var benchTime = new CPUTimeBenchmark();
		benchTime.plus(Duration.ofMillis(time), EnumSet.of(TIME.REAL));

		return new explainStats()
		{
			@Override
			public CPUTimeBenchmark getTime()
			{
				return benchTime;
			}

			@Override
			public long getNbAnswers()
			{
				return nbAnswers;
			}
		};
	}
	// ==========================================================================

	private static Collection<CPUTimeBenchmark> streamMeasures = TheDemo.getMeasures(TheMeasures.QEVAL_STREAM_NEXT, TheMeasures.QEVAL_STREAM_TOTAL).values();

	private void inhibitBatch_start()
	{
		CPUTimeBenchmark.stopChrono(streamMeasures);
		inhibitTime.startChrono();
	}

	private void inhibitBatch_end()
	{
		inhibitTime.stopChrono();
		CPUTimeBenchmark.startChrono(streamMeasures);
	}

	private <T> Stream<List<T>> batchIt(Stream<T> stream, int batchSize, long[] nbItems)
	{
		var batch = HelpStream.batch(stream, batchSize, nbItems);

		if (inhibitBatchStreamTime)
			batch = HelpStream.clamp(batch, this::inhibitBatch_start, this::inhibitBatch_end);

		return batch;
	}

	// ==========================================================================

	private long[] nbQueries;

	@Override
	public Stream<Object> execute(Stream<ITree<Object, KVLabel>> queries)
	{
		nbQueries = new long[] { 0, 0 };
		var batch = batchIt(queries.map(this::tree2Query), queryBatchSize, nbQueries);

		return batch.flatMap(this::executeBson);
	}

	@Override
	public Stream<Object> explain(Stream<ITree<Object, KVLabel>> queries)
	{
		nbQueries = new long[] { 0, 0 };
		var batch = batchIt(queries.map(this::tree2Query), queryBatchSize, nbQueries);

		return batch.flatMap(this::explainBson);
	}

	@Override
	public long getNbQueries()
	{
		return nbQueries[0];
	}

	@Override
	public long getNbBatches()
	{
		return nbQueries[1];
	}

	@Override
	public Stream<Object> executeNatives(Stream<Object> nativeQueries)
	{
		nbQueries = new long[] { 0, 0 };
		var batch = batchIt(nativeQueries, queryBatchSize, nbQueries);

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

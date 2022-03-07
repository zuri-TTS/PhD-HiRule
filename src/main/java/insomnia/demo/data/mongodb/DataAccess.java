package insomnia.demo.data.mongodb;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.bag.HashBag;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Triple;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
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
import insomnia.data.ITreeNavigator;
import insomnia.demo.TheDemo;
import insomnia.demo.TheDemo.TheMeasures;
import insomnia.demo.data.IDataAccess;
import insomnia.demo.input.Summary;
import insomnia.implem.data.TreeTypeNavigators;
import insomnia.implem.data.Trees;
import insomnia.implem.data.creational.TreeBuilder;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.kv.data.KVValues;
import insomnia.implem.summary.ILabelSummary;
import insomnia.implem.summary.LabelTypeSummary;
import insomnia.implem.summary.PathSummary;
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
		Q2NATIVE_SUMMARY(Option.builder().longOpt("toNative.summary").desc("(str) Path to the summary to use for the query2native traduction").build()), //
		Q2NATIVE_SUMMARY_TYPE(Option.builder().longOpt("toNative.summary.type").desc("(str) key-type|path").build()), //
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

	private String summaryUrl, summaryType;

	private ITreeNavigator<EnumSet<NodeType>, KVLabel> summaryNavigator;

	private DataAccess(URI uri, Configuration config)
	{
		connection = new ConnectionString(uri.toString());

		client     = MongoClients.create(connection);
		collection = client.getDatabase(connection.getDatabase()).getCollection(connection.getCollection());

		queryBatchSize         = config.getInt(MyOptions.QUERY_BATCHSIZE.opt.getLongOpt(), 100);
		dataBatchSize          = config.getInt(MyOptions.DATA_BATCHSIZE.opt.getLongOpt(), 100);
		checkTerminalLeaf      = config.getBoolean(MyOptions.LEAF_CHECKTERMINAL.opt.getLongOpt(), true);
		inhibitBatchStreamTime = config.getBoolean(MyOptions.INHIBIT_BATCH_STREAM_TIME.opt.getLongOpt(), true);
		q2NativeDots           = config.getBoolean(MyOptions.Q2NATIVE_DOTS.opt.getLongOpt(), false);

		if (q2NativeDots)
			throw new UnsupportedOperationException("'q2NativeDots' functionnality is disabled");

		if (inhibitBatchStreamTime)
			inhibitTime = TheDemo.measure(TheMeasures.QEVAL_STREAM_INHIB);

		summaryUrl       = config.getString(MyOptions.Q2NATIVE_SUMMARY.opt.getLongOpt(), "");
		summaryType      = config.getString(MyOptions.Q2NATIVE_SUMMARY_TYPE.opt.getLongOpt());
		summaryNavigator = null;
	}

	private void needSummary()
	{
		if (!summaryUrl.isEmpty())
		{
			if (q2NativeDots)
				throw new IllegalArgumentException(String.format("%s cannot be set with %s", //
					MyOptions.Q2NATIVE_DOTS.opt.getLongOpt(), //
					MyOptions.Q2NATIVE_SUMMARY.opt.getLongOpt()//
				));

			try
			{
				var summary = Summary.get(summaryUrl, Summary.parseType(summaryType));

				if (summary instanceof PathSummary<?, ?>)
					summaryNavigator = TreeTypeNavigators.from((PathSummary<Object, KVLabel>) summary, true);
				else if (summary instanceof LabelTypeSummary<?, ?>)
					summaryNavigator = TreeTypeNavigators.constant((LabelTypeSummary<Object, KVLabel>) summary, true);
				else if (summary instanceof ILabelSummary<?, ?>)
					summaryNavigator = TreeTypeNavigators.constant(EnumSet.of(NodeType.ARRAY));
				else
					throw new IllegalArgumentException(String.format("[mongodb] Can't handle the summary: %s", summary));
			}
			catch (IOException | ParseException e)
			{
				throw new Error(e);
			}
		}
		else
			summaryNavigator = TreeTypeNavigators.constant(EnumSet.of(NodeType.ARRAY));
	}

	private ITreeNavigator<EnumSet<NodeType>, KVLabel> getSummaryNavigator()
	{
		if (null == summaryNavigator)
			needSummary();

		return summaryNavigator;
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
		BsonDocument filter = tree2Query(tree, tree.getRoot());
		q2native.stopChrono();
		return filter;
	}

	private boolean isInt(double val)
	{
		return val == Math.ceil(val);
	}

	private BsonDocument tree2Query(ITree<Object, KVLabel> tree, INode<Object, KVLabel> node)
	{
		return tree2Query(tree, node, EnumSet.of(NodeType.OBJECT));
	}

	private static boolean isExists_op(BsonValue val)
	{
		return doc_exists.equals(val) || doc_texists.equals(val);
	}

	private static BsonDocument bsonEmptyDocument = new BsonDocument();

	private static BsonDocument combineExpr(BsonDocument a, BsonDocument b)
	{
		if (bsonEmptyDocument.equals(a) || bsonEmptyDocument.equals(b))
			return bsonEmptyDocument;
		if (isExists_op(a))
			return b;
		if (isExists_op(b))
			return a;

		var     eqa   = a.get("$eq");
		var     eqb   = b.get("$eq");
		boolean isEqa = null != eqa;
		boolean isEqb = null != eqb;

		if (isEqa && isEqb)
			return new BsonDocument("$all", new BsonArray(List.of(eqa, eqb)));

		// Let a be the non-scalar value
		if (isEqa)
		{
			var tmp2 = a;
			a = b;
			b = tmp2;
		}

		if (!a.isDocument())
			return bsonEmptyDocument;

		var adoc = a.asDocument();
		var all  = adoc.get("$all");

		if (null != all)
		{
			var array = all.asArray().clone();
			array.add(eqb);
			return new BsonDocument("$all", array);
		}
		return bsonEmptyDocument;
	}

	private static BsonDocument doc_exists = new BsonDocument("$exists", BsonBoolean.TRUE);

	private static BsonDocument leafBadType = new BsonDocument("$type", new BsonArray( //
		List.of(new BsonInt32(BsonType.ARRAY.getValue()), new BsonInt32(BsonType.DOCUMENT.getValue())) //
	));

	private static BsonDocument doc_texists = new BsonDocument() //
		.append("$exists", BsonBoolean.TRUE) //
		.append("$not", leafBadType);

	private BsonDocument tree2Query( //
		ITree<Object, KVLabel> tree, INode<Object, KVLabel> node, EnumSet<NodeType> type)
	{
		var summaryNavigator = getSummaryNavigator();
		var childs           = tree.getChildren(node);
		var nbChilds         = childs.size();

		if (0 == nbChilds)
			return documentFromNodeValue(node, type);
		else
		{
			if (type.containsAll(List.of(NodeType.ARRAY, NodeType.OBJECT)))
				throw new Error(String.format("[mongodb] a label must have only one type: have %s", type.toString()));

			List<BsonDocument> bsonChilds = new ArrayList<>();
			Bag<String>        keyBag     = new HashBag<>();

			var currentPos = summaryNavigator.getCurrentNode();

			{
				var value = node.getValue();

				if (value != null && !KVValues.interpretation().isAny(value))
					throw new Error(String.format("Intermediary node has a value: %s", value));
			}
			for (var c : childs)
			{
				var label       = c.getLabel();
				var labelPrefix = label.asString() + ".";

				summaryNavigator.setCurrentNode(currentPos);
				summaryNavigator.followFirstPath(List.of(label));
				var doc = tree2Query(tree, c.getChild(), summaryNavigator.getCurrentNode().getValue());
				{
					var res = objectMergeChilds(doc, labelPrefix);
					doc = (null != res) ? res : new BsonDocument(label.asString(), doc);
				}
				if (doc.isDocument())
					keyBag.addAll(doc.keySet());

				bsonChilds.add(doc);
			}
			BsonDocument ret;

			if (keyBag.stream().anyMatch(k -> keyBag.getCount(k) > 1))
			{
				ret = deduplicateChilds(bsonChilds, keyBag);

				if (null == ret)
					ret = makeAnd(bsonChilds);
			}
			else
			{
				ret = new BsonDocument();

				for (var d : bsonChilds)
					for (var k : d.keySet())
						ret.append(k, simplifyEq(d.get(k)));
			}

			if (type.contains(NodeType.ARRAY))
				ret = new BsonDocument("$elemMatch", ret);

			return ret;
		}
	}

	private static BsonValue simplifyOne(BsonValue doc, String key)
	{
		if (!doc.isDocument())
			return doc;

		var eqv = doc.asDocument().get(key);
		return null == eqv ? doc : eqv;
	}

	private static BsonValue simplifyEq(BsonValue doc)
	{
		return simplifyOne(doc, "$eq");
	}

	private BsonDocument documentFromNodeValue(INode<Object, KVLabel> node, EnumSet<NodeType> type)
	{
		var value = node.getValue();

		if (value == null || KVValues.interpretation().isAny(value))
		{
			if (node.isTerminal() && checkTerminalLeaf)
			{
				if (type.contains(NodeType.ARRAY))
					return new BsonDocument("$elemMatch", new BsonDocument("$not", leafBadType));
				else
					return doc_texists;
			}
			else
				return doc_exists;
		}
		BsonValue bsonValue;

		if (value instanceof String)
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

		return new BsonDocument("$eq", bsonValue);
	}

	private static BsonDocument objectMergeChilds(BsonDocument childDoc, String labelPrefix)
	{
		var res = new BsonDocument();
		var and = childDoc.get("$and");

		// Get up a $and element
		if (null != and)
		{
			var array = new BsonArray();

			for (var obj : and.asArray())
			{
				var doc = obj.asDocument();
				var k   = doc.getFirstKey();
				array.add(new BsonDocument(labelPrefix + k, doc.get(k)));
			}
			return new BsonDocument("$and", array);
		}
		else
		{
			for (var k : childDoc.keySet())
			{
				if (k.startsWith("$"))
					return null;

				res.append(labelPrefix + k, childDoc.get(k));
			}
		}
		return res;
	}

	private static BsonDocument makeAnd(List<BsonDocument> docs)
	{
		var array = new BsonArray();

		// simplify $eq
		for (var doc : docs)
		{
			if (doc.size() == 1)
			{
				var k = doc.getFirstKey();
				array.add(new BsonDocument(k, simplifyEq(doc.get(k))));
			}
			else
				array.add(doc);
		}
		return new BsonDocument("$and", array);
	}

	private static BsonDocument deduplicateChilds(List<BsonDocument> bsonChilds, Bag<String> keyBag)
	{
		var res = new BsonDocument();

		MultiValuedMap<String, BsonDocument> duplicates = new HashSetValuedHashMap<>();

		for (var doc : bsonChilds)
		{
			for (var k : doc.keySet())
			{
				if (keyBag.getCount(k) == 1)
					res.append(k, simplifyEq(doc.get(k)));
				else
					duplicates.put(k, doc.getDocument(k));
			}
		}

		for (var dup : duplicates.asMap().entrySet())
		{
			var key   = dup.getKey();
			var dedup = dup.getValue().stream().reduce(DataAccess::combineExpr);

			if (dedup.get().equals(bsonEmptyDocument))
				return null;

			res.append(key, dedup.get());
		}
		return res;
	}

	@Override
	public long getNbDocuments()
	{
		return collection.estimatedDocumentCount();
	}

	@Override
	public Stream<ITree<Object, KVLabel>> all()
	{
		var cursor = collection.find();

		if (dataBatchSize > 0)
			cursor.batchSize(dataBatchSize);

		return HelpStream.toStream(cursor) //
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

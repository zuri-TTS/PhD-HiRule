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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.bag.HashBag;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.tuple.Pair;
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
import insomnia.demo.Measures;
import insomnia.demo.TheConfiguration;
import insomnia.demo.TheDemo;
import insomnia.demo.TheDemo.TheMeasures;
import insomnia.demo.data.IDataAccess;
import insomnia.demo.input.LogicalPartition;
import insomnia.demo.input.Summary;
import insomnia.implem.data.TreeFilters;
import insomnia.implem.data.TreeFilters.NodeInfos;
import insomnia.implem.data.TreeTypeNavigators;
import insomnia.implem.data.Trees;
import insomnia.implem.data.creational.TreeBuilder;
import insomnia.implem.kv.data.KVLabel;
import insomnia.implem.kv.data.KVLabels;
import insomnia.implem.kv.data.KVValues;
import insomnia.implem.summary.LabelSummary;
import insomnia.implem.summary.PathSummary;
import insomnia.lib.cpu.CPUTimeBenchmark;
import insomnia.lib.cpu.CPUTimeBenchmark.TIME;
import insomnia.lib.help.HelpStream;
import insomnia.lib.numeric.MultiInterval;
import insomnia.summary.ISummary.NodeType;

public final class DataAccess implements IDataAccess<Object, KVLabel>
{
	private enum MyOptions
	{
		QUERY_BATCHSIZE(Option.builder().longOpt("query.batchSize").desc("(int) How many queries to send at once to MongoDB").build()), //
		BATCHES_NBTHREADS(Option.builder().longOpt("query.batches.nbThreads").desc("(int) Number of thread used to process batches").build()), //
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

	// For now this can only handle one mongo client
	private static MongoClient client;

	private ConnectionString connection;

	private MongoCollection<Document> collection;

	private LogicalPartition logicalPartition;

	private String partitionID;

	private int queryBatchSize, dataBatchSize;

	private int nbThreads;

	private boolean checkTerminalLeaf;

	private boolean inhibitBatchStreamTime;

	private boolean q2NativeDots;

	private Predicate<ITree<Object, KVLabel>> queryFilter;

	private CPUTimeBenchmark inhibitTime;

	private String summaryUrl, summaryType;

	private String collectionName;

	private ITreeNavigator<NodeInfos<Object>, KVLabel> summaryNavigator;

	private CPUTimeBenchmark q2native;

	private static Collection<CPUTimeBenchmark> streamMeasures;

	private Measures measures;

	private DataAccess(URI uri, Configuration config, String db, String collectionName, Measures measures)
	{
		if (null == client)
		{
			var nbcolls = config.getList("db.collection").size();
			var uri_s   = uri.toString();

			if (!uri_s.contains("/?"))
				uri_s = uri_s + "/?";

			uri_s      += "&maxConnecting=" + nbcolls;
			connection  = new ConnectionString(uri_s);
			client      = MongoClients.create(connection);
		}
		this.measures       = measures;
		logicalPartition    = LogicalPartition.nullValue();
		collection          = client.getDatabase(db).getCollection(collectionName);
		this.collectionName = collectionName;
		this.q2native       = measures.getTime(TheDemo.TheMeasures.QUERY_TO_NATIVE.measureName());
		streamMeasures      = List.of( //
			measures.getTime(TheMeasures.QEVAL_STREAM_NEXT.measureName()), //
			measures.getTime(TheMeasures.QEVAL_STREAM_TOTAL.measureName()) //
		);

		partitionID            = config.getString(TheConfiguration.OneProperty.PartitionID.getPropertyName());
		queryBatchSize         = config.getInt(MyOptions.QUERY_BATCHSIZE.opt.getLongOpt(), 100);
		dataBatchSize          = config.getInt(MyOptions.DATA_BATCHSIZE.opt.getLongOpt(), 100);
		nbThreads              = config.getInt(MyOptions.BATCHES_NBTHREADS.opt.getLongOpt(), 1);
		checkTerminalLeaf      = config.getBoolean(MyOptions.LEAF_CHECKTERMINAL.opt.getLongOpt(), true);
		inhibitBatchStreamTime = config.getBoolean(MyOptions.INHIBIT_BATCH_STREAM_TIME.opt.getLongOpt(), true);
		q2NativeDots           = config.getBoolean(MyOptions.Q2NATIVE_DOTS.opt.getLongOpt(), false);

		if (q2NativeDots)
			throw new UnsupportedOperationException("'q2NativeDots' functionnality is disabled");

		if (inhibitBatchStreamTime)
			inhibitTime = measures.getTime(TheMeasures.QEVAL_STREAM_INHIB.measureName());

		summaryUrl       = config.getString(MyOptions.Q2NATIVE_SUMMARY.opt.getLongOpt(), "");
		summaryType      = config.getString(MyOptions.Q2NATIVE_SUMMARY_TYPE.opt.getLongOpt());
		summaryNavigator = null;
	}

	@Override
	public String getCollectionName()
	{
		return collectionName;
	}

	@Override
	public LogicalPartition getLogicalPartition()
	{
		return logicalPartition;
	}

	@Override
	public void setLogicalPartition(LogicalPartition partition)
	{
		logicalPartition = partition;
	}

	@Override
	public void setQueryFilter(Predicate<ITree<Object, KVLabel>> filter)
	{
		this.queryFilter = filter;
	}

	private ITreeNavigator<NodeInfos<Object>, KVLabel> createSummaryNavigator()
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
				var summary = Summary.get(summaryUrl, Summary.parseType(summaryType), EnumSet.noneOf(TreeFilters.Filters.class));

				if (summary instanceof PathSummary<?, ?>)
					return TreeTypeNavigators.from((PathSummary<Object, KVLabel>) summary, true);
				else if (summary instanceof LabelSummary<?, ?>)
					return TreeTypeNavigators.constant((LabelSummary<Object, KVLabel>) summary, true);
				else
					throw new IllegalArgumentException(String.format("[mongodb] Can't handle the summary: %s", summary));
			}
			catch (IOException | ParseException e)
			{
				throw new Error(e);
			}
		}
		else
			return TreeTypeNavigators.constant(EnumSet.of(NodeType.MULTIPLE));
	}

	private ITreeNavigator<NodeInfos<Object>, KVLabel> getSummaryNavigator()
	{
		if (null == summaryNavigator)
			summaryNavigator = createSummaryNavigator();

		return summaryNavigator;
	}

	// ==========================================================================

	public static IDataAccess<Object, KVLabel> open(URI uri, Configuration config, String db, String collection, Measures measures)
	{
		return new DataAccess(uri, config, db, collection, measures);
	}

	private static ITree<Object, KVLabel> doc2Tree(Document doc)
	{
		var tb = new TreeBuilder<Object, KVLabel>();
		tb.setRooted();
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
		}
		else if (doc.isString())
		{
			sb.setValue(doc.asString().getValue()).setTerminal();
			sb.goUp();
		}
		else if (doc.isNumber())
		{
			sb.setValue(Double.valueOf(doc.asNumber().doubleValue())).setTerminal();
			sb.goUp();
		}
		else if (doc.isNull())
			sb.setTerminal().goUp();
		else if (doc.isObjectId())
		{
			sb.setValue(doc.asObjectId().getValue().toString()).setTerminal();
			sb.goUp();
		}
		else
			throw new IllegalArgumentException(String.format("Cannot handle %s value", doc));
	}

	private Bson tree2Query(ITree<Object, KVLabel> tree)
	{
		return tree2Query(tree, getSummaryNavigator());
	}

	private Bson tree2Query(ITree<Object, KVLabel> tree, ITreeNavigator<NodeInfos<Object>, KVLabel> summaryNavigator)
	{
		q2native.startChrono();
		BsonDocument filter = tree2Query_(tree, summaryNavigator);
		q2native.stopChrono();
		return filter;
	}

	private boolean isInt(double val)
	{
		return val == Math.ceil(val);
	}

	private BsonDocument tree2Query_(ITree<Object, KVLabel> tree, ITreeNavigator<NodeInfos<Object>, KVLabel> summaryNavigator)
	{
		summaryNavigator.goToRoot();

		if (!logicalPartition.getInterval().isNull())
		{
			var tbuilder = new TreeBuilder<>(tree);
			tbuilder.addChildDown(0).setLabel(KVLabels.create(partitionID)).setValue(logicalPartition.getInterval()).setTerminal();
			tree = Trees.create(tbuilder);
		}
		return tree2Query(tree, tree.getRoot(), summaryNavigator, EnumSet.of(NodeType.OBJECT));
	}

	private static boolean isExists_op(BsonValue val)
	{
		return doc_exists.equals(val) || doc_texists.equals(val);
	}

	private static BsonDocument bsonEmptyDocument = new BsonDocument();

	private static BsonDocument combineExpr(String key, BsonDocument a, BsonDocument b)
	{
		if (bsonEmptyDocument.equals(a) || bsonEmptyDocument.equals(b))
			return bsonEmptyDocument;
		if (isExists_op(a))
			return b;
		if (isExists_op(b))
			return a;

		// Do not merge a path key
		if (key.contains("."))
			return bsonEmptyDocument;

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
		ITree<Object, KVLabel> tree, INode<Object, KVLabel> node, ITreeNavigator<NodeInfos<Object>, KVLabel> summaryNavigator, EnumSet<NodeType> type)
	{
		var childs   = tree.getChildren(node);
		var nbChilds = childs.size();

		if (0 == nbChilds)
			return documentFromNodeValue(node, type);
		else
		{
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
				var doc = tree2Query(tree, c.getChild(), summaryNavigator, summaryNavigator.getCurrentNode().getValue().getNodeTypes());
				{
					var res = objectMergeChilds(doc, labelPrefix);
					doc = (null != res) ? res : new BsonDocument(label.asString(), doc);
				}
				if (doc.isDocument())
					keyBag.addAll(doc.keySet());

				bsonChilds.add(doc);
			}
			BsonDocument ret;

			// Has some childs with the same label
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
						ret.append(k, d.get(k));
			}

			if (NodeType.isMultiple(type))
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
				if (NodeType.isMultiple(type))
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
		else if (value instanceof MultiInterval)
		{
			var filters = partitionFilters(logicalPartition);

			// TODO
			if (filters.size() != 1)
				throw new UnsupportedOperationException(String.format("For now it's impossible to handle more than one partition's Interval; has %s", logicalPartition));

			return filters.get(0);
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
				array.add(new BsonDocument(k, doc.get(k)));
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
			var dedup = dup.getValue().stream().reduce((a, b) -> combineExpr(key, a, b));

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

	private static List<BsonDocument> partitionFilters(LogicalPartition partition)
	{
		var filters = new ArrayList<BsonDocument>();

		for (var interval : partition.getInterval().getIntervals())
		{
			if (interval.lsize() == 1)
				filters.add(new BsonDocument() //
					.append("$eq", new BsonInt32((int) interval.getMin()))); //
			else
				filters.add(new BsonDocument() //
					.append("$gte", new BsonInt32((int) interval.getMin())) //
					.append("$lte", new BsonInt32((int) interval.getMax())));
		}
		return filters;
	}

	private Bson partitionFilter(LogicalPartition partition)
	{
		var filters = partitionFilters(partition);

		if (filters.size() == 1)
		{
			return new BsonDocument(partitionID, filters.get(0));
		}
		else
		{
			for (int i = 0, c = filters.size(); i < c; i++)
				filters.set(i, new BsonDocument(partitionID, filters.get(i)));

			return new BsonDocument("$or", new BsonArray(filters));
		}
	}

	@Override
	public Stream<Object> all()
	{
		FindIterable<Document> cursor;

		if (!logicalPartition.getInterval().isNull())
			cursor = collection.find(partitionFilter(logicalPartition));
		else if (!logicalPartition.isNull())
		{
			summaryNavigator = TreeTypeNavigators.constant(EnumSet.of(NodeType.OBJECT));
			var prefixTree = logicalPartition.getPrefix();
			cursor           = collection.find(tree2Query(prefixTree));
			summaryNavigator = null;
		}
		else
			cursor = collection.find();

		if (dataBatchSize > 0)
			cursor.batchSize(dataBatchSize);

		return HelpStream.<Object>toStreamDownCast(cursor);
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

	public Stream<ITree<Object, KVLabel>> wrapQueries(Stream<ITree<Object, KVLabel>> queries)
	{
		if (queryFilter != null)
			return queries.filter(queryFilter);

		return queries;
	}

	@Override
	public Stream<Triple<ITree<Object, KVLabel>, Object, Stream<Object>>> executeEach(Stream<ITree<Object, KVLabel>> queries)
	{
		queries = wrapQueries(queries);
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

		var cursor = collection.find(disjunction);
		var stats = cursor.explain(ExplainVerbosity.EXECUTION_STATS);
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
	public explainStats explainStats(Object record)
	{
		if (record instanceof explainStats)
			return (explainStats) record;

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

	private explainStats emptyStats()
	{
		var cpu = new CPUTimeBenchmark();

		return new explainStats()
		{
			@Override
			public CPUTimeBenchmark getTime()
			{
				return cpu;
			}

			@Override
			public long getNbAnswers()
			{
				return 0;
			}
		};
	}

	private explainStats addStats(explainStats stats, Object stats_b)
	{
		var  b         = explainStats(stats_b);
		var  benchTime = CPUTimeBenchmark.plus(stats.getTime(), b.getTime());
		long nbAnswers = stats.getNbAnswers() + b.getNbAnswers();

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
		queries   = wrapQueries(queries);
		var batch = batchIt(queries.map(this::tree2Query), queryBatchSize, nbQueries);

		return batch.flatMap(this::executeBson);
	}

	@Override
	public Stream<Object> explain(Stream<ITree<Object, KVLabel>> queries)
	{
		if (nbThreads < 1)
			throw new IllegalArgumentException(String.format("%s must be positive. Have %d", MyOptions.BATCHES_NBTHREADS.opt.getLongOpt(), nbThreads));

		queries = wrapQueries(queries);

		if (nbThreads == 1)
		{
			nbQueries = new long[] { 0, 0 };
			var batch = batchIt(queries.map(this::tree2Query), queryBatchSize, nbQueries);

			return batch.flatMap(this::explainBson);
		}
		else
			return explainParallel(queries);
	}

	private boolean explainParallel_end;

	private Stream<Object> explainParallel(Stream<ITree<Object, KVLabel>> queries)
	{
		try
		{
			explainParallel_end = false;

			var queriesQueue = new ArrayBlockingQueue<ITree<Object, KVLabel>>(queryBatchSize * nbThreads);
			var futures      = new ArrayList<Future<Pair<explainStats, Measures>>>(nbThreads);
			var execService  = Executors.newFixedThreadPool(nbThreads);

			for (int i = 0; i < nbThreads; i++)
				futures.add(execService.submit(explainCallable(queriesQueue, i)));

			execService.shutdown();
			var threadMeas = measures.getTime("threads.time");

			measures.set("threads", "nb", nbThreads);

			threadMeas.startChrono();

			for (var q : HelpStream.toIterable(queries))
				queriesQueue.put(Trees.create(q));

			explainParallel_end = true;

			while (!execService.isTerminated())
				execService.awaitTermination(10, TimeUnit.SECONDS);

			threadMeas.stopChrono();

			var stats = new ArrayList<Object>(nbThreads);
			nbQueries = new long[] { 0, 0 };
			int i = 0;

			for (var f : futures)
			{
				i++;
				var res       = f.get();
				var rStats    = res.getKey();
				var rmeasures = res.getValue();

				rmeasures.setPrefix(String.format("thread.%d.", i));
				measures.addAll(rmeasures);

				stats.add(rStats);
				int qtotal  = rmeasures.getInt("queries", "total");
				int batchNb = rmeasures.getInt("queries", "batch.nb");

				nbQueries[0] += qtotal;
				nbQueries[1] += batchNb;
			}
			return stats.stream();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new Error(e);
		}
	}

	public Callable<Pair<explainStats, Measures>> explainCallable(BlockingQueue<ITree<Object, KVLabel>> queries, int i)
	{
		return () -> {
			var threadMeasures = new Measures();
			var threadTime     = threadMeasures.getTime("thread", "time");

			var tqueriesList = new ArrayList<ITree<Object, KVLabel>>(queryBatchSize);
			var queriesList  = new ArrayList<Bson>(queryBatchSize);
			var count        = 0;

			var summaryNavigator = createSummaryNavigator();

			explainStats totalStats = emptyStats();

			int nbBatches = 0, nbQueries = 0;

			threadTime.startChrono();
			while (!explainParallel_end || !queries.isEmpty())
			{
				count += queries.drainTo(tqueriesList, queryBatchSize - tqueriesList.size());

				if (count == 0)
					continue;

				if ((explainParallel_end && count != 0) || count == queryBatchSize)
				{
					nbQueries += count;
					nbBatches++;

					var stats = explainBson(tqueriesList.stream().map(t -> tree2Query(t, summaryNavigator)).collect(Collectors.toList())) //
						.collect(Collectors.toList()).get(0);
					totalStats = addStats(totalStats, stats);

					queriesList.clear();
					tqueriesList.clear();
					count = 0;
				}
			}
			threadMeasures.set("queries", "total", nbQueries);
			threadMeasures.set("queries", "batch.nb", nbBatches);
			threadMeasures.set("answers", "total", (int) totalStats.getNbAnswers());
			threadMeasures.set(TheDemo.TheMeasures.QEVAL_STATS_DB_TIME.measureName(), totalStats.getTime());
			threadTime.stopChrono();
			return Pair.of(totalStats, threadMeasures);
		};
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
	public long getRecordId(Object record)
	{
		return ((Document) record).getInteger(partitionID);
	}

	@Override
	public void writeInfos(PrintWriter printer)
	{
		printer.println(collection.getNamespace().getFullName());
		printer.printf("nb docs: %d\n", collection.countDocuments());
	}
}

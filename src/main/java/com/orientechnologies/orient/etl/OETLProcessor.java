/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.etl.block.OBlock;
import com.orientechnologies.orient.etl.extractor.OExtractor;
import com.orientechnologies.orient.etl.loader.OLoader;
import com.orientechnologies.orient.etl.loader.OOrientDBLoader;
import com.orientechnologies.orient.etl.source.OSource;
import com.orientechnologies.orient.etl.transformer.OTransformer;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * ETL processor class.
 * 
 * @author Luca Garulli (l.garulli-at-orientechnologies.com)
 */
public class OETLProcessor implements OETLComponent {
	protected final List<OBlock> beginBlocks;
	protected final List<OBlock> endBlocks;
	protected final OSource source;
	protected final OExtractor extractor;
	protected final OLoader loader;
	protected final List<OTransformer> transformers;
	protected final OETLComponentFactory factory = new OETLComponentFactory();
	protected final OBasicCommandContext context;
	protected long startTime;
	protected long elapsed;
	protected OETLProcessorStats stats = new OETLProcessorStats();
	protected TimerTask dumpTask;
	protected LOG_LEVELS logLevel = LOG_LEVELS.INFO;
	protected boolean parallel = false;
	protected int maxRetries = 10;
	private Thread[] threads;

	public enum LOG_LEVELS {
		NONE, ERROR, INFO, DEBUG
	}

	public class OETLProcessorStats {
		public long lastExtractorProgress = 0;
		public long lastLoaderProgress = 0;
		public long lastLap = 0;
		public AtomicLong warnings = new AtomicLong();
		public AtomicLong errors = new AtomicLong();

		public long incrementWarnings() {
			return warnings.incrementAndGet();
		}

		public long incrementErrors() {
			return errors.incrementAndGet();
		}
	}

	public OETLProcessor(final List<OBlock> iBeginBlocks,
			final OSource iSource, final OExtractor iExtractor,
			final List<OTransformer> iTransformers, final OLoader iLoader,
			final List<OBlock> iEndBlocks, final OBasicCommandContext iContext) {
		beginBlocks = iBeginBlocks;
		source = iSource;
		extractor = iExtractor;
		transformers = iTransformers;
		loader = iLoader;
		endBlocks = iEndBlocks;
		context = iContext;
		init();
	}

	public OETLProcessor(final Collection<ODocument> iBeginBlocks,
			final ODocument iSource, final ODocument iExtractor,
			final Collection<ODocument> iTransformers, final ODocument iLoader,
			final Collection<ODocument> iEndBlocks,
			final OBasicCommandContext iContext) {
		context = iContext;
		init();

		try {
			String name;

			// BEGIN BLOCKS
			beginBlocks = new ArrayList<OBlock>();
			if (iBeginBlocks != null)
				for (ODocument block : iBeginBlocks) {
					name = block.fieldNames()[0];
					final OBlock b = factory.getBlock(name);
					beginBlocks.add(b);
					configureComponent(b, (ODocument) block.field(name),
							iContext);
					b.execute();
				}

			if (iSource != null) {
				// SOURCE
				name = iSource.fieldNames()[0];
				source = factory.getSource(name);
				configureComponent(source, (ODocument) iSource.field(name),
						iContext);
			} else
				source = factory.getSource("input");

			// EXTRACTOR
			name = iExtractor.fieldNames()[0];
			extractor = factory.getExtractor(name);
			configureComponent(extractor, (ODocument) iExtractor.field(name),
					iContext);

			if (iLoader != null) {
				// LOADER
				name = iLoader.fieldNames()[0];
				loader = factory.getLoader(name);
				configureComponent(loader, (ODocument) iLoader.field(name),
						iContext);
			} else
				loader = factory.getLoader("output");

			// TRANSFORMERS
			transformers = new ArrayList<OTransformer>();
			if (iTransformers != null)
				for (ODocument t : iTransformers) {
					name = t.fieldNames()[0];
					final OTransformer tr = factory.getTransformer(name);
					transformers.add(tr);
					configureComponent(tr, (ODocument) t.field(name), iContext);
				}

			// END BLOCKS
			endBlocks = new ArrayList<OBlock>();
			if (iEndBlocks != null)
				for (ODocument block : iEndBlocks) {
					name = block.fieldNames()[0];
					final OBlock b = factory.getBlock(name);
					endBlocks.add(b);
					configureComponent(b, (ODocument) block.field(name),
							iContext);
				}

			// analyzeFlow();

		} catch (Exception e) {
			throw new OConfigurationException(
					"Error on creating ETL processor", e);
		}
	}

	public static List<Object> executeSubETL(final String[] args) {
		final OETLProcessor processor = createProcessor(args);

		processor.execute();

		return ((OOrientDBLoader) processor.loader).getLoadedObjects();
	}

	public static void main(final String[] args) {
		System.out.println("OrientDB etl v." + OConstants.getVersion() + " "
				+ OConstants.ORIENT_URL);

		final OETLProcessor processor = createProcessor(args);
		processor.execute();
	}

	private static OETLProcessor createProcessor(String[] args) {
		ODocument cfgGlobal = null;
		Collection<ODocument> cfgBegin = null;
		ODocument cfgSource = null;
		ODocument cfgExtract = null;
		Collection<ODocument> cfgTransformers = null;
		ODocument cfgLoader = null;
		Collection<ODocument> cfgEnd = null;

		if (args.length == 0) {
			System.out.println("Syntax error, missing configuration file.");
			System.out.println("Use: oetl.sh <json-file>");
			System.exit(1);
		}

		final OBasicCommandContext context = createDefaultContext();

		for (int i = 0; i < args.length; ++i) {
			final String arg = args[i];

			if (arg.charAt(0) == '-') {
				final String[] parts = arg.substring(1).split("=");
				context.setVariable(parts[0].toUpperCase(), parts[1]);
			} else {
				try {
					final String config = OIOUtils.readFileAsString(new File(
							arg));
					final ODocument cfg = new ODocument().fromJSON(config,
							"noMap");

					cfgGlobal = cfg.field("config");
					cfgBegin = cfg.field("begin");
					cfgSource = cfg.field("source");
					cfgExtract = cfg.field("extractor");
					cfgTransformers = cfg.field("transformers");
					cfgLoader = cfg.field("loader");
					cfgEnd = cfg.field("end");

				} catch (IOException e) {
					throw new OConfigurationException(
							"Error on loading config file: " + arg);
				}
			}
		}

		if (cfgExtract == null)
			throw new IllegalArgumentException("No Extractor configured");

		if (cfgGlobal != null) {
			// INIT ThE CONTEXT WITH GLOBAL CONFIGURATION
			for (String f : cfgGlobal.fieldNames()) {
				context.setVariable(f, cfgGlobal.field(f));
			}
		}

		return new OETLProcessor(cfgBegin, cfgSource, cfgExtract,
				cfgTransformers, cfgLoader, cfgEnd, context);
	}

	protected static OBasicCommandContext createDefaultContext() {
		final OBasicCommandContext context = new OBasicCommandContext();
		context.setVariable("dumpEveryMs", 1000);
		return context;
	}

	protected static Collection<ODocument> parseTransformers(final String value) {
		final ArrayList<ODocument> cfgTransformers = new ArrayList<ODocument>();
		if (!value.isEmpty()) {
			if (value.charAt(0) == '{') {
				cfgTransformers.add((ODocument) new ODocument().fromJSON(value,
						"noMap"));
			} else if (value.charAt(0) == '[') {
				final ArrayList<String> items = new ArrayList<String>();
				OStringSerializerHelper.getCollection(value, 0, items);
				for (String item : items)
					cfgTransformers.add((ODocument) new ODocument().fromJSON(
							item, "noMap"));
			}
		}
		return cfgTransformers;
	}

	public OETLComponentFactory getFactory() {
		return factory;
	}

	public void execute() {
		if (parallel)
			executeParallel();
		else
			executeSequentially();
	}

	public void executeParallel() {
		try {
			begin();

			out(LOG_LEVELS.INFO, "Started parallel execution with %d threads",
					threads.length);

			if (source != null) {
				final Reader reader = source.read();

				if (reader != null)
					extractor.extract(reader);
			}

			final LinkedBlockingQueue<OExtractedItem> queue = new LinkedBlockingQueue<OExtractedItem>(
					threads.length * 500) {
				@Override
				public boolean offer(OExtractedItem e) {
					// turn offer() and add() into a blocking calls (unless
					// interrupted)
					try {
						put(e);
						return true;
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
					return false;
				}
			};

			final AtomicLong counter = new AtomicLong();
			final AtomicBoolean extractionFinished = new AtomicBoolean(false);
			final OETLProcessor processor = this;

			for (int i = 0; i < threads.length; ++i) {
				threads[i] = new Thread(new Runnable() {
					@Override
					public void run() {
						final OETLPipeline pipeline = new OETLPipeline(
								processor, transformers, loader, logLevel,
								maxRetries);
						pipeline.begin();

						while (!extractionFinished.get() || counter.get() > 0) {
							try {
								final OExtractedItem content = queue.take();
								try {
									pipeline.execute(content);
								} finally {
									counter.decrementAndGet();
								}
							} catch (InterruptedException e) {
							}
						}
					}
				}, "OrientDB ETL pipeline-" + i);

				threads[i].setDaemon(true);
				threads[i].start();
			}

			while (extractor.hasNext()) {
				// EXTRACTOR
				final OExtractedItem current = extractor.next();

				// TRANSFORM + LOAD
				queue.offer(current);
				counter.incrementAndGet();
			}

			extractionFinished.set(true);

			while (counter.get() > 0) {
				out(LOG_LEVELS.INFO,
						"Waiting for the pipeline to finish, remaining "
								+ counter.get() + " entries to process");
				try {
					// WAIT A BIT AND RETRY
					Thread.sleep(500);
				} catch (InterruptedException e) {
				}
			}

			end();

		} catch (OETLProcessHaltedException e) {
			out(LOG_LEVELS.ERROR, "ETL process halted: %s", e);
		}
	}

	@Override
	public ODocument getConfiguration() {
		return null;
	}

	@Override
	public void configure(OETLProcessor iProcessor,
			final ODocument iConfiguration, OBasicCommandContext iSettings) {
	}

	@Override
	public void begin() {
		out(LOG_LEVELS.INFO, "BEGIN ETL PROCESSOR");

		final Integer cfgMaxRetries = (Integer) context
				.getVariable("maxRetries");
		if (cfgMaxRetries != null)
			maxRetries = cfgMaxRetries;

		final int dumpEveryMs = (Integer) context.getVariable("dumpEveryMs");
		if (dumpEveryMs > 0) {
			dumpTask = new TimerTask() {
				@Override
				public void run() {
					dumpProgress();
				}
			};

			Orient.instance().getTimer()
					.schedule(dumpTask, dumpEveryMs, dumpEveryMs);

			startTime = System.currentTimeMillis();
		}

		for (OBlock t : beginBlocks) {
			t.begin();
			t.execute();
			t.end();
		}

		if (source != null)
			source.begin();
		extractor.begin();
	}

	@Override
	public void end() {
		for (OTransformer t : transformers)
			t.end();

		if (source != null)
			source.end();
		extractor.end();
		loader.end();

		for (OBlock t : endBlocks) {
			t.begin();
			t.execute();
			t.end();
		}

		elapsed = System.currentTimeMillis() - startTime;
		if (dumpTask != null) {
			dumpTask.cancel();
		}

		out(LOG_LEVELS.INFO, "END ETL PROCESSOR");

		dumpProgress();
	}

	@Override
	public String getName() {
		return "Processor";
	}

	public void out(final LOG_LEVELS iLogLevel, final String iText,
			final Object... iArgs) {
		if (logLevel.ordinal() >= iLogLevel.ordinal())
			System.out.println(String.format(iText, iArgs));
	}

	public OETLProcessorStats getStats() {
		return stats;
	}

	public OExtractor getExtractor() {
		return extractor;
	}

	public OLoader getLoader() {
		return loader;
	}

	public List<OTransformer> getTransformers() {
		return transformers;
	}

	public LOG_LEVELS getLogLevel() {
		return logLevel;
	}

	public OBasicCommandContext getContext() {
		return context;
	}

	protected void executeSequentially() {
		try {
			begin();

			if (source != null) {
				final Reader reader = source.read();

				if (reader != null)
					extractor.extract(reader);
			}

			final OETLPipeline pipeline = new OETLPipeline(this, transformers,
					loader, logLevel, maxRetries);
			pipeline.begin();

			while (extractor.hasNext()) {
				// EXTRACTOR
				final OExtractedItem current = extractor.next();

				// TRANSFORM + LOAD
				pipeline.execute(current);
			}

			end();

		} catch (OETLProcessHaltedException e) {
			out(LOG_LEVELS.ERROR, "ETL process halted: %s", e);
		}
	}

	protected void configureComponent(final OETLComponent iComponent,
			final ODocument iCfg, final OBasicCommandContext iContext) {
		iComponent.configure(this, iCfg, iContext);
	}

	protected Class getClassByName(final OETLComponent iComponent,
			final String iClassName) {
		final Class inClass;
		if (iClassName.equals("ODocument"))
			inClass = ODocument.class;
		else if (iClassName.equals("String"))
			inClass = String.class;
		else if (iClassName.equals("Object"))
			inClass = Object.class;
		else if (iClassName.equals("OrientVertex"))
			inClass = OrientVertex.class;
		else if (iClassName.equals("OrientEdge"))
			inClass = OrientEdge.class;
		else
			try {
				inClass = Class.forName(iClassName);
			} catch (ClassNotFoundException e) {
				throw new OConfigurationException("Class '" + iClassName
						+ "' declared as 'input' of ETL Component '"
						+ iComponent.getName() + "' was not found.");
			}
		return inClass;
	}

	protected void dumpProgress() {
		final long now = System.currentTimeMillis();

		final long extractorProgress = extractor.getProgress();
		final long extractorTotal = extractor.getTotal();
		final long extractorItemsSec = (long) ((extractorProgress - stats.lastExtractorProgress) * 1000f / (now - stats.lastLap));
		final String extractorUnit = extractor.getUnit();

		final long loaderProgress = loader.getProgress();
		final long loaderItemsSec = (long) ((loaderProgress - stats.lastLoaderProgress) * 1000f / (now - stats.lastLap));
		final String loaderUnit = loader.getUnit();

		final String extractorTotalFormatted = extractorTotal > -1 ? String
				.format("%,d", extractorTotal) : "?";

		if (extractorTotal == -1) {
			out(LOG_LEVELS.INFO,
					"+ extracted %,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
					extractorProgress, extractorUnit, extractorItemsSec,
					extractorUnit, extractor.getProgress(),
					extractor.getUnit(), loaderProgress, loaderUnit,
					loaderItemsSec, loaderUnit,
					OIOUtils.getTimeAsString(now - startTime),
					stats.warnings.get(), stats.errors.get());
		} else {
			float extractorPercentage = ((float) extractorProgress * 100 / extractorTotal);

			out(LOG_LEVELS.INFO,
					"+ %3.2f%% -> extracted %,d/%,d %s (%,d %s/sec) - %,d %s -> loaded %,d %s (%,d %s/sec) Total time: %s [%d warnings, %d errors]",
					extractorPercentage, extractorProgress, extractorTotal,
					extractorUnit, extractorItemsSec, extractorUnit,
					extractor.getProgress(), extractor.getUnit(),
					loaderProgress, loaderUnit, loaderItemsSec, loaderUnit,
					OIOUtils.getTimeAsString(now - startTime),
					stats.warnings.get(), stats.errors.get());
		}

		stats.lastExtractorProgress = extractorProgress;
		stats.lastLoaderProgress = loaderProgress;
		stats.lastLap = now;
	}

	protected void analyzeFlow() {
		if (extractor == null)
			throw new OConfigurationException("extractor is null");

		if (loader == null)
			throw new OConfigurationException("loader is null");

		OETLComponent lastComponent = extractor;

		for (OTransformer t : transformers) {
			checkTypeCompatibility(t, lastComponent);
			lastComponent = t;
		}

		checkTypeCompatibility(loader, lastComponent);
	}

	protected void checkTypeCompatibility(
			final OETLComponent iCurrentComponent,
			final OETLComponent iLastComponent) {
		final String out;
		final List<String> ins;

		try {
			out = iLastComponent.getConfiguration().field("output");
			if (out == null)
				// SKIP IT
				return;

			ins = iCurrentComponent.getConfiguration().field("input");
			if (ins == null)
				// SKIP IT
				return;

			final Class outClass = getClassByName(iLastComponent, out);

			for (String in : ins) {
				final Class inClass = getClassByName(iCurrentComponent, in);
				if (inClass.isAssignableFrom(outClass)) {
					return;
				}
			}

		} catch (Exception e) {
			throw new OConfigurationException(
					"Error on checking compatibility between components '"
							+ iLastComponent.getName() + "' and '"
							+ iCurrentComponent.getName() + "'", e);
		}

		throw new OConfigurationException("Component '"
				+ iCurrentComponent.getName()
				+ "' expects one of the following inputs " + ins
				+ " but the 'output' for component '"
				+ iLastComponent.getName() + "' is: " + out);

	}

	protected void init() {
		final String cfgLog = (String) context.getVariable("log");
		if (cfgLog != null)
			logLevel = LOG_LEVELS.valueOf(cfgLog.toUpperCase());

		final Object parallelSetting = context.getVariable("parallel");
		if (parallelSetting != null)
			parallel = ((Boolean) parallelSetting).booleanValue();

		if (parallel) {
			final int cores = Runtime.getRuntime().availableProcessors();
			threads = new Thread[cores];
			for (int i = 0; i < cores; ++i) {
				threads[i] = new Thread("OrientDB ETL Pipeline-" + i);
			}
		}
	}
}

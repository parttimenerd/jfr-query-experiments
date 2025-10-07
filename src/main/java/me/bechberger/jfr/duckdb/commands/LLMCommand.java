package me.bechberger.jfr.duckdb.commands;

import dev.langchain4j.agent.tool.ReturnBehavior;
import dev.langchain4j.agent.tool.Tool;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.agent.tool.ToolSpecifications;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.chat.response.CompleteToolCall;
import dev.langchain4j.model.chat.response.PartialToolCall;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.ollama.OllamaChatModel;
import dev.langchain4j.model.ollama.OllamaStreamingChatModel;
import dev.langchain4j.observability.api.event.AiServiceResponseReceivedEvent;
import dev.langchain4j.observability.api.listener.AiServiceResponseReceivedListener;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.SystemMessage;
import me.bechberger.jfr.duckdb.BasicParallelImporter;
import me.bechberger.jfr.duckdb.Options;
import me.bechberger.jfr.duckdb.query.QueryExecutor;
import org.duckdb.DuckDBConnection;
import org.jooq.User;
import picocli.CommandLine;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@CommandLine.Command(name = "llm", description = "Generate ideas using LLM")
public class LLMCommand implements Runnable {

    enum LLMModel {
        QWEN3_SMALL("qwen3"),
        QWEN3_MEDIUM("qwen3:14b"),
        QWEN3_LARGE("qwen3:30b");
        final String name;

        LLMModel(String name) {
            this.name = name;
        }
    }

    @CommandLine.Parameters(index = "0", description = "The JFR file to query.")
    private String inputFile;

    @CommandLine.Option(names = "--use-examples")
    private boolean useExamplesForLLM = false;

    @CommandLine.Option(names = "--model", description = "The LLM model to use. Supported models: ${COMPLETION-CANDIDATES}", defaultValue = "QWEN3_SMALL")
    private LLMModel model;

    @CommandLine.Option(names = "model-url")
    private String modelUrl = "http://localhost:11434";

    @CommandLine.Mixin
    private Options commonOptions;

    @Override
    public void run() {
        try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            commonOptions.setUseExamplesForLLM(useExamplesForLLM);
            BasicParallelImporter.CreationResult result = BasicParallelImporter.importIntoConnection(Path.of(inputFile), conn, commonOptions);
            runWithDB(result, conn);
        } catch (Exception e) {
            throw new RuntimeException("Error importing JFR file: " + inputFile, e);
        }
    }

    static class Tools {

        private QueryExecutor executor;

        public Tools(QueryExecutor executor) {
            this.executor = executor;
        }

        @Tool("Sums 2 given numbers")
        double sum(double a, double b) {
            System.out.println("Summing " + a + " and " + b);
            return a + b;
        }

        @Tool
        double squareRoot(double x) {
            System.out.println("Calculating square root of " + x);
            return Math.sqrt(x);
        }

        @Tool("Executes the given SQL query and returns the result as a csv")
        String executeSQL(String sql) {
            System.out.println("Executing SQL: " + sql);
            try {
                return executor.executeQuery(sql, QueryExecutor.OutputFormat.CSV, 30);
            } catch (Exception e) {
                return "Error executing SQL: " + e.getMessage() + "\n" + (e.getCause() != null ? e.getCause().getMessage() : "");
            }
        }
    }

    interface DBQueryAgent {

       @SystemMessage("""
               You're a helpful duckdb database agent that helps to query a duckdb database.
               You put all table names in double quotes. You call the tools often.
               SELECT * FROM hot-methods LIMIT 10   -> SELECT * FROM "hot-methods" LIMIT 10 (hyphens in names must be quoted)
               There are the following tables in the database (described via the SQL definitions):
               
               CREATE TABLE "StringFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" VARCHAR,
                 "origin" VARCHAR
               );
               CREATE TABLE "PhysicalMemory" ("startTime" TIMESTAMP,
                 "totalSize" BIGINT,
                 "usedSize" BIGINT
               );
               CREATE TABLE "ClassLoadingStatistics" ("startTime" TIMESTAMP,
                 "loadedClassCount" BIGINT,
                 "unloadedClassCount" BIGINT
               );
               CREATE TABLE "GCConfiguration" ("startTime" TIMESTAMP,
                 "youngCollector" VARCHAR,
                 "oldCollector" VARCHAR,
                 "parallelGCThreads" INTEGER,
                 "concurrentGCThreads" INTEGER,
                 "usesDynamicGCThreads" BOOLEAN,
                 "isExplicitGCConcurrent" BOOLEAN,
                 "isExplicitGCDisabled" BOOLEAN,
                 "pauseTarget" DOUBLE,
                 "gcTimeRatio" INTEGER
               );
               CREATE TABLE "OSInformation" ("startTime" TIMESTAMP,
                 "osVersion" VARCHAR
               );
               CREATE TABLE "YoungGarbageCollection" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "gcId" INTEGER,
                 "tenuringThreshold" INTEGER
               );
               CREATE TABLE "MetaspaceChunkFreeListSummary" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "when" VARCHAR,
                 "metadataType" VARCHAR,
                 "specializedChunks" BIGINT,
                 "specializedChunksTotalSize" BIGINT,
                 "smallChunks" BIGINT,
                 "smallChunksTotalSize" BIGINT,
                 "mediumChunks" BIGINT,
                 "mediumChunksTotalSize" BIGINT,
                 "humongousChunks" BIGINT,
                 "humongousChunksTotalSize" BIGINT
               );
               CREATE TABLE "G1EvacuationYoungStatistics" ("startTime" TIMESTAMP,
                 "statistics$gcId" INTEGER,
                 "statistics$allocated" BIGINT,
                 "statistics$wasted" BIGINT,
                 "statistics$used" BIGINT,
                 "statistics$undoWaste" BIGINT,
                 "statistics$regionEndWaste" BIGINT,
                 "statistics$regionsRefilled" INTEGER,
                 "statistics$numPlabsFilled" BIGINT,
                 "statistics$directAllocated" BIGINT,
                 "statistics$numDirectAllocated" BIGINT,
                 "statistics$failureUsed" BIGINT,
                 "statistics$failureWaste" BIGINT
               );
               CREATE TABLE "NativeLibraryLoad" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "name" VARCHAR,
                 "success" BOOLEAN,
                 "errorMessage" VARCHAR
               );
               CREATE TABLE "GCPhasePause" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "name" VARCHAR
               );
               CREATE TABLE "DirectBufferStatistics" ("startTime" TIMESTAMP,
                 "maxCapacity" BIGINT,
                 "count" BIGINT,
                 "totalCapacity" BIGINT,
                 "memoryUsed" BIGINT
               );
               CREATE TABLE "ObjectAllocationSample" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "objectClass" INTEGER -- references Class(_id) if != 0,
                 "weight" BIGINT
               );
               CREATE TABLE "CPUTimeStampCounter" ("startTime" TIMESTAMP,
                 "fastTimeEnabled" BOOLEAN,
                 "fastTimeAutoEnabled" BOOLEAN,
                 "osFrequency" BIGINT,
                 "fastTimeFrequency" BIGINT
               );
               CREATE TABLE "NativeLibrary" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "baseAddress" BIGINT,
                 "topAddress" BIGINT
               );
               CREATE TABLE "IntFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" INTEGER,
                 "origin" VARCHAR
               );
               CREATE TABLE "VirtualizationInformation" ("startTime" TIMESTAMP,
                 "name" VARCHAR
               );
               CREATE TABLE "ThreadStart" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "thread" INTEGER -- references Thread(_id) if != 0,
                 "parentThread" INTEGER -- references Thread(_id) if != 0
               );
               CREATE TABLE "CompilerStatistics" ("startTime" TIMESTAMP,
                 "compileCount" INTEGER,
                 "bailoutCount" INTEGER,
                 "invalidatedCount" INTEGER,
                 "osrCompileCount" INTEGER,
                 "standardCompileCount" INTEGER,
                 "osrBytesCompiled" BIGINT,
                 "standardBytesCompiled" BIGINT,
                 "nmethodsSize" BIGINT,
                 "nmethodCodeSize" BIGINT,
                 "peakTimeSpent" DOUBLE,
                 "totalTimeSpent" DOUBLE
               );
               CREATE TABLE "CompilerQueueUtilization" ("startTime" TIMESTAMP,
                 "compiler" VARCHAR,
                 "addedRate" BIGINT,
                 "removedRate" BIGINT,
                 "queueSize" BIGINT,
                 "peakQueueSize" BIGINT,
                 "addedCount" BIGINT,
                 "removedCount" BIGINT,
                 "totalAddedCount" BIGINT,
                 "totalRemovedCount" BIGINT,
                 "compilerThreadCount" INTEGER
               );
               CREATE TABLE "MetaspaceSummary" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "when" VARCHAR,
                 "gcThreshold" BIGINT,
                 "metaspace$committed" BIGINT,
                 "metaspace$used" BIGINT,
                 "metaspace$reserved" BIGINT,
                 "dataSpace$committed" BIGINT,
                 "dataSpace$used" BIGINT,
                 "dataSpace$reserved" BIGINT,
                 "classSpace$committed" BIGINT,
                 "classSpace$used" BIGINT,
                 "classSpace$reserved" BIGINT
               );
               CREATE TABLE "InitialSecurityProperty" ("startTime" TIMESTAMP,
                 "key" VARCHAR,
                 "value" VARCHAR
               );
               CREATE TABLE "GCHeapMemoryUsage" ("startTime" TIMESTAMP,
                 "used" BIGINT,
                 "committed" BIGINT,
                 "max" BIGINT
               );
               CREATE TABLE "ActiveRecording" ("startTime" TIMESTAMP,
                 "id" BIGINT,
                 "name" VARCHAR,
                 "destination" VARCHAR,
                 "disk" BOOLEAN,
                 "maxAge" DOUBLE,
                 "flushInterval" DOUBLE,
                 "maxSize" BIGINT,
                 "recordingStart" TIMESTAMP,
                 "recordingDuration" DOUBLE
               );
               CREATE TABLE "LongFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" BIGINT,
                 "origin" VARCHAR
               );
               CREATE TABLE "GCPhaseConcurrentLevel1" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "name" VARCHAR
               );
               CREATE TABLE "UnsignedIntFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" INTEGER,
                 "origin" VARCHAR
               );
               CREATE TABLE "ModuleRequire" ("startTime" TIMESTAMP,
                 "source" INTEGER -- references Module(_id) if != 0,
                 "requiredModule" INTEGER -- references Module(_id) if != 0
               );
               CREATE TABLE "TenuringDistribution" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "age" INTEGER,
                 "size" BIGINT
               );
               CREATE TABLE "JavaMonitorWait" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "monitorClass" INTEGER -- references Class(_id) if != 0,
                 "notifier" INTEGER -- references Thread(_id) if != 0,
                 "timeout" DOUBLE,
                 "timedOut" BOOLEAN,
                 "address" BIGINT
               );
               CREATE TABLE "DoubleFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" DOUBLE,
                 "origin" VARCHAR
               );
               CREATE TABLE "GCPhasePauseLevel2" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "name" VARCHAR
               );
               CREATE TABLE "ThreadContextSwitchRate" ("startTime" TIMESTAMP,
                 "switchRate" FLOAT
               );
               CREATE TABLE "ThreadEnd" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "thread" INTEGER -- references Thread(_id) if != 0
               );
               CREATE TABLE "ThreadCPULoad" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "user" FLOAT,
                 "system" FLOAT
               );
               CREATE TABLE "ModuleExport" ("startTime" TIMESTAMP,
                 "exportedPackage" INTEGER -- references Package(_id) if != 0,
                 "targetModule" INTEGER -- references Module(_id) if != 0
               );
               CREATE TABLE "ExecutionSample" ("startTime" TIMESTAMP,
                 "sampledThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "state" VARCHAR
               );
               CREATE TABLE "JVMInformation" ("startTime" TIMESTAMP,
                 "jvmName" VARCHAR,
                 "jvmVersion" VARCHAR,
                 "jvmArguments" VARCHAR,
                 "jvmFlags" VARCHAR,
                 "javaArguments" VARCHAR,
                 "jvmStartTime" TIMESTAMP,
                 "pid" BIGINT
               );
               CREATE TABLE "ExecuteVMOperation" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "operation" VARCHAR,
                 "safepoint" BOOLEAN,
                 "blocking" BOOLEAN,
                 "caller" INTEGER -- references Thread(_id) if != 0,
                 "safepointId" BIGINT
               );
               CREATE TABLE "G1EvacuationOldStatistics" ("startTime" TIMESTAMP,
                 "statistics$gcId" INTEGER,
                 "statistics$allocated" BIGINT,
                 "statistics$wasted" BIGINT,
                 "statistics$used" BIGINT,
                 "statistics$undoWaste" BIGINT,
                 "statistics$regionEndWaste" BIGINT,
                 "statistics$regionsRefilled" INTEGER,
                 "statistics$numPlabsFilled" BIGINT,
                 "statistics$directAllocated" BIGINT,
                 "statistics$numDirectAllocated" BIGINT,
                 "statistics$failureUsed" BIGINT,
                 "statistics$failureWaste" BIGINT
               );
               CREATE TABLE "GCSurvivorConfiguration" ("startTime" TIMESTAMP,
                 "maxTenuringThreshold" TINYINT,
                 "initialTenuringThreshold" TINYINT
               );
               CREATE TABLE "CPUInformation" ("startTime" TIMESTAMP,
                 "cpu" VARCHAR,
                 "description" VARCHAR,
                 "sockets" INTEGER,
                 "cores" INTEGER,
                 "hwThreads" INTEGER
               );
               CREATE TABLE "JavaErrorThrow" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "message" VARCHAR,
                 "thrownClass" INTEGER -- references Class(_id) if != 0
               );
               CREATE TABLE "BooleanFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" BOOLEAN,
                 "origin" VARCHAR
               );
               CREATE TABLE "GCPhaseConcurrent" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "name" VARCHAR
               );
               CREATE TABLE "JavaThreadStatistics" ("startTime" TIMESTAMP,
                 "activeCount" BIGINT,
                 "daemonCount" BIGINT,
                 "accumulatedCount" BIGINT,
                 "peakCount" BIGINT
               );
               CREATE TABLE "G1MMU" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "timeSlice" DOUBLE,
                 "gcTime" DOUBLE,
                 "pauseTarget" DOUBLE
               );
               CREATE TABLE "Shutdown" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "reason" VARCHAR
               );
               CREATE TABLE "G1AdaptiveIHOP" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "threshold" BIGINT,
                 "thresholdPercentage" FLOAT,
                 "ihopTargetOccupancy" BIGINT,
                 "currentOccupancy" BIGINT,
                 "additionalBufferSize" BIGINT,
                 "predictedAllocationRate" DOUBLE,
                 "predictedMarkingDuration" DOUBLE,
                 "predictionActive" BOOLEAN
               );
               CREATE TABLE "MetaspaceGCThreshold" ("startTime" TIMESTAMP,
                 "oldValue" BIGINT,
                 "newValue" BIGINT,
                 "updater" VARCHAR
               );
               CREATE TABLE "ActiveSetting" ("startTime" TIMESTAMP,
                 "id" BIGINT,
                 "name" VARCHAR,
                 "value" VARCHAR
               );
               CREATE TABLE "CompilerConfiguration" ("startTime" TIMESTAMP,
                 "threadCount" INTEGER,
                 "tieredCompilation" BOOLEAN,
                 "dynamicCompilerThreadCount" BOOLEAN
               );
               CREATE TABLE "ClassLoaderStatistics" ("startTime" TIMESTAMP,
                 "classLoader" INTEGER -- references ClassLoader(_id) if != 0,
                 "parentClassLoader" INTEGER -- references ClassLoader(_id) if != 0,
                 "classLoaderData" BIGINT,
                 "classCount" BIGINT,
                 "chunkSize" BIGINT,
                 "blockSize" BIGINT,
                 "hiddenClassCount" BIGINT,
                 "hiddenChunkSize" BIGINT,
                 "hiddenBlockSize" BIGINT
               );
               CREATE TABLE "CodeCacheConfiguration" ("startTime" TIMESTAMP,
                 "initialSize" BIGINT,
                 "reservedSize" BIGINT,
                 "nonNMethodSize" BIGINT,
                 "profiledSize" BIGINT,
                 "nonProfiledSize" BIGINT,
                 "expansionSize" BIGINT,
                 "minBlockLength" BIGINT,
                 "startAddress" BIGINT,
                 "reservedTopAddress" BIGINT
               );
               CREATE TABLE "UnsignedLongFlag" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "value" BIGINT,
                 "origin" VARCHAR
               );
               CREATE TABLE "ThreadPark" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "parkedClass" INTEGER -- references Class(_id) if != 0,
                 "timeout" DOUBLE,
                 "until" TIMESTAMP,
                 "address" BIGINT
               );
               CREATE TABLE "G1GarbageCollection" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "gcId" INTEGER,
                 "type" VARCHAR
               );
               CREATE TABLE "GCHeapMemoryPoolUsage" ("startTime" TIMESTAMP,
                 "name" VARCHAR,
                 "used" BIGINT,
                 "committed" BIGINT,
                 "max" BIGINT
               );
               CREATE TABLE "DeprecatedInvocation" ("startTime" TIMESTAMP,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "method" INTEGER -- references Method(_id) if != 0,
                 "invocationTime" TIMESTAMP,
                 "forRemoval" BOOLEAN
               );
               CREATE TABLE "G1HeapSummary" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "when" VARCHAR,
                 "edenUsedSize" BIGINT,
                 "edenTotalSize" BIGINT,
                 "survivorUsedSize" BIGINT,
                 "oldGenUsedSize" BIGINT,
                 "numberOfRegions" INTEGER
               );
               CREATE TABLE "ResidentSetSize" ("startTime" TIMESTAMP,
                 "size" BIGINT,
                 "peak" BIGINT
               );
               CREATE TABLE "GCReferenceStatistics" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "type" VARCHAR,
                 "count" BIGINT
               );
               CREATE TABLE "YoungGenerationConfiguration" ("startTime" TIMESTAMP,
                 "minSize" BIGINT,
                 "maxSize" BIGINT,
                 "newRatio" INTEGER
               );
               CREATE TABLE "StringTableStatistics" ("startTime" TIMESTAMP,
                 "bucketCount" BIGINT,
                 "entryCount" BIGINT,
                 "totalFootprint" BIGINT,
                 "bucketCountMaximum" BIGINT,
                 "bucketCountAverage" FLOAT,
                 "bucketCountVariance" FLOAT,
                 "bucketCountStandardDeviation" FLOAT,
                 "insertionRate" FLOAT,
                 "removalRate" FLOAT
               );
               CREATE TABLE "GarbageCollection" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "name" VARCHAR,
                 "cause" VARCHAR,
                 "sumOfPauses" DOUBLE,
                 "longestPause" DOUBLE
               );
               CREATE TABLE "InitialSystemProperty" ("startTime" TIMESTAMP,
                 "key" VARCHAR,
                 "value" VARCHAR
               );
               CREATE TABLE "GCPhasePauseLevel1" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "name" VARCHAR
               );
               CREATE TABLE "NetworkUtilization" ("startTime" TIMESTAMP,
                 "networkInterface" VARCHAR,
                 "readRate" BIGINT,
                 "writeRate" BIGINT
               );
               CREATE TABLE "ThreadDump" ("startTime" TIMESTAMP,
                 "result" VARCHAR
               );
               CREATE TABLE "SystemProcess" ("startTime" TIMESTAMP,
                 "pid" VARCHAR,
                 "commandLine" VARCHAR
               );
               CREATE TABLE "GCHeapConfiguration" ("startTime" TIMESTAMP,
                 "minSize" BIGINT,
                 "maxSize" BIGINT,
                 "initialSize" BIGINT,
                 "usesCompressedOops" BOOLEAN,
                 "compressedOopsMode" VARCHAR,
                 "objectAlignment" BIGINT,
                 "heapAddressBits" TINYINT
               );
               CREATE TABLE "OldGarbageCollection" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "gcId" INTEGER
               );
               CREATE TABLE "CPULoad" ("startTime" TIMESTAMP,
                 "jvmUser" FLOAT,
                 "jvmSystem" FLOAT,
                 "machineTotal" FLOAT
               );
               CREATE TABLE "OldObjectSample" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "allocationTime" TIMESTAMP,
                 "objectSize" BIGINT,
                 "objectAge" DOUBLE,
                 "lastKnownHeapUsage" BIGINT,
                 "object" INTEGER -- references OldObject(_id) if != 0,
                 "arrayElements" INTEGER,
                 "root" INTEGER -- references OldObjectGcRoot(_id) if != 0
               );
               CREATE TABLE "GCPhaseParallel" ("startTime" TIMESTAMP,
                 "duration" DOUBLE,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "gcId" INTEGER,
                 "gcWorkerId" INTEGER,
                 "name" VARCHAR
               );
               CREATE TABLE "EvacuationInformation" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "cSetRegions" INTEGER,
                 "cSetUsedBefore" BIGINT,
                 "cSetUsedAfter" BIGINT,
                 "allocationRegions" INTEGER,
                 "allocationRegionsUsedBefore" BIGINT,
                 "allocationRegionsUsedAfter" BIGINT,
                 "bytesCopied" BIGINT,
                 "regionsFreed" INTEGER
               );
               CREATE TABLE "CodeCacheStatistics" ("startTime" TIMESTAMP,
                 "codeBlobType" VARCHAR,
                 "startAddress" BIGINT,
                 "reservedTopAddress" BIGINT,
                 "entryCount" INTEGER,
                 "methodCount" INTEGER,
                 "adaptorCount" INTEGER,
                 "unallocatedCapacity" BIGINT,
                 "fullCount" INTEGER
               );
               CREATE TABLE "ExceptionStatistics" ("startTime" TIMESTAMP,
                 "throwables" BIGINT
               );
               CREATE TABLE "GCCPUTime" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "userTime" DOUBLE,
                 "systemTime" DOUBLE,
                 "realTime" DOUBLE
               );
               CREATE TABLE "Deoptimization" ("startTime" TIMESTAMP,
                 "eventThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "compileId" INTEGER,
                 "compiler" VARCHAR,
                 "method" INTEGER -- references Method(_id) if != 0,
                 "lineNumber" INTEGER,
                 "bci" INTEGER,
                 "instruction" VARCHAR,
                 "reason" VARCHAR,
                 "action" VARCHAR
               );
               CREATE TABLE "ThreadAllocationStatistics" ("startTime" TIMESTAMP,
                 "allocated" BIGINT,
                 "thread" INTEGER -- references Thread(_id) if != 0
               );
               CREATE TABLE "NativeMethodSample" ("startTime" TIMESTAMP,
                 "sampledThread" INTEGER -- references Thread(_id) if != 0,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "state" VARCHAR
               );
               CREATE TABLE "GCHeapSummary" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "when" VARCHAR,
                 "heapSpace$start" BIGINT,
                 "heapSpace$committedEnd" BIGINT,
                 "heapSpace$committedSize" BIGINT,
                 "heapSpace$reservedEnd" BIGINT,
                 "heapSpace$reservedSize" BIGINT,
                 "heapUsed" BIGINT
               );
               CREATE TABLE "SymbolTableStatistics" ("startTime" TIMESTAMP,
                 "bucketCount" BIGINT,
                 "entryCount" BIGINT,
                 "totalFootprint" BIGINT,
                 "bucketCountMaximum" BIGINT,
                 "bucketCountAverage" FLOAT,
                 "bucketCountVariance" FLOAT,
                 "bucketCountStandardDeviation" FLOAT,
                 "insertionRate" FLOAT,
                 "removalRate" FLOAT
               );
               CREATE TABLE "InitialEnvironmentVariable" ("startTime" TIMESTAMP,
                 "key" VARCHAR,
                 "value" VARCHAR
               );
               CREATE TABLE "G1BasicIHOP" ("startTime" TIMESTAMP,
                 "gcId" INTEGER,
                 "threshold" BIGINT,
                 "thresholdPercentage" FLOAT,
                 "targetOccupancy" BIGINT,
                 "currentOccupancy" BIGINT,
                 "recentMutatorAllocationSize" BIGINT,
                 "recentMutatorDuration" DOUBLE,
                 "recentAllocationRate" DOUBLE,
                 "lastMarkingDuration" DOUBLE
               );
               CREATE TABLE "MetaspaceAllocationFailure" ("startTime" TIMESTAMP,
                 "stackTrace$topMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topApplicationMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$topNonInitMethod" INTEGER -- references Method(_id) if != 0,
                 "stackTrace$length" SHORT,
                 "stackTrace$truncated" BOOLEAN,
                 "stackTrace$methods" INTEGER[10] -- references Method(_id) if != 0,
                 "classLoader" INTEGER -- references ClassLoader(_id) if != 0,
                 "hiddenClassLoader" BOOLEAN,
                 "size" BIGINT,
                 "metadataType" VARCHAR,
                 "metaspaceObjectType" VARCHAR
               );
               CREATE TABLE "GCTLABConfiguration" ("startTime" TIMESTAMP,
                 "usesTLABs" BOOLEAN,
                 "minTLABSize" BIGINT,
                 "tlabRefillWasteLimit" BIGINT
               );
               CREATE TABLE "Method" (_id INTEGER PRIMARY KEY,
                 "type" INTEGER -- references Class(_id) if != 0,
                 "name" VARCHAR,
                 "descriptor" VARCHAR,
                 "modifiers" INTEGER,
                 "hidden" BOOLEAN
               );
               CREATE TABLE "Module" (_id INTEGER PRIMARY KEY,
                 "name" VARCHAR,
                 "version" VARCHAR,
                 "location" VARCHAR,
                 "classLoader" INTEGER -- references ClassLoader(_id) if != 0
               );
               CREATE TABLE "Thread" (_id INTEGER PRIMARY KEY,
                 "osName" VARCHAR,
                 "osThreadId" BIGINT,
                 "javaName" VARCHAR,
                 "javaThreadId" BIGINT,
                 "group" INTEGER -- references ThreadGroup(_id) if != 0,
                 "virtual" BOOLEAN
               );
               CREATE TABLE "ClassLoader" (_id INTEGER PRIMARY KEY,
                 "name" VARCHAR,
                 "javaName" VARCHAR
               );
               CREATE TABLE "ThreadGroup" (_id INTEGER PRIMARY KEY,
                 "name" VARCHAR
               );
               CREATE TABLE "OldObjectField" (_id INTEGER PRIMARY KEY,
                 "name" VARCHAR,
                 "modifiers" SMALLINT
               );
               CREATE TABLE "Class" (_id INTEGER PRIMARY KEY,
                 "classLoader" INTEGER -- references ClassLoader(_id) if != 0,
                 "name" VARCHAR,
                 "package" INTEGER -- references Package(_id) if != 0,
                 "modifiers" INTEGER,
                 "hidden" BOOLEAN,
                 "javaName" VARCHAR
               );
               CREATE TABLE "Reference" (_id INTEGER PRIMARY KEY,
                 "array$size" INTEGER,
                 "array$index" INTEGER,
                 "field" INTEGER -- references OldObjectField(_id) if != 0,
                 "skip" INTEGER
               );
               CREATE TABLE "Package" (_id INTEGER PRIMARY KEY,
                 "name" VARCHAR,
                 "module" INTEGER -- references Module(_id) if != 0,
                 "exported" BOOLEAN
               );
               CREATE TABLE "OldObject" (_id INTEGER PRIMARY KEY,
                 "address" BIGINT,
                 "type" INTEGER -- references Class(_id) if != 0,
                 "description" VARCHAR,
                 "referrer" INTEGER -- references Reference(_id) if != 0
               );
               CREATE TABLE "OldObjectGcRoot" (_id INTEGER PRIMARY KEY,
                 "description" VARCHAR,
                 "system" VARCHAR,
                 "type" VARCHAR
               );
               
               Additionally, there are the following views and macros:
               
               The following SQL macros are available for use in queries:
               - P90(col): 90th percentile of a column.
               - P95(col): 95th percentile of a column.
               - P99(col): 99th percentile of a column.
               - P999(col): 99.9th percentile of a column.
               - normalized(x): Normalize a value to [0,1] over entire input, by comparing with max value, might have problems with LIMIT
               - diff(col): Row delta: col - LAG(col) OVER(ORDER BY col).
               - JFR_UNIQUE(x): Count distinct values (JFR UNIQUE(x)).
               - format_decimals(num, decimals): Format number with fixed number of decimals.
               - format_percentage(num, decimals := 2): Format a number as percentage with fixed number of decimals.
               - format_memory(bytes, decimals := 2): Format bytes into human readable string (B, KB, MB, GB, TB).
               - format_human_duration(sec): Format seconds into human readable string).
               - format_duration(seconds, decimals := 2): Format seconds using SI units (s, ms, us, ns) with specified decimal places. Does not go larger than seconds.
               - format_hex(i): Format integer as hex string (with 0x prefix).
               - before_gc(ts): GC id of the last GC before the event, or -1. Slow on large tables.
               - after_gc(ts): GC id of the first GC after the event, or -1. Slow on large tables.
               - duration_since_last_gc(ts): Duration since the last GC before the event, or -1.
               - HEAP_BEFORE_GC(gc_id): Get heap usage before GC for a given GC ID.
               - HEAP_AFTER_GC(gc_id): Get heap usage after GC for a given GC ID.
               - GC_TYPE(gc_id): Get GC type for a given GC ID (Young/Old/Unknown).
               - EVENT_TYPE_LABEL(et): Get the event label for the event table name.
               - EVENT_NAME_FOR_ID(_id): Get the event table name for the event ID.
               - macro_sql(macro_name): Get the SQL definition of a macro by name.
               Use these macros to simplify your SQL queries on JFR data. Use 'macro_sql(macro_name)' to get the SQL definition of a macro.
               
               The available views with their descriptions are:
               - active-recordings: Shows all active recordings with their start, duration and name.
               - active-settings: Shows the active settings for all event types.
               - allocation-by-class: Shows the classes which have the highest allocation pressure.
               - allocation-by-thread: Shows the threads which have the highest allocation pressure.
               - allocation-by-site: Shows the methods which have the highest allocation pressure.
               - class-loaders: Shows all class loaders with their loaded class count and hidden class count.
               - compiler-configuration: Shows the configuration of the JIT compiler.
               - compiler-statistics: Shows statistics about the JIT compiler.
               - deprecated-methods-for-removal: Shows all deprecated methods which are marked for removal and the classes from which they are called.
               - cpu-information: Shows information about the CPU(s) on which the JVM is running.
               - cpu-load: Shows statistics about the CPU load of the JVM and the machine.
               - cpu-load-samples: Shows the CPU load samples of the JVM and the machine over time.
               - cpu-tsc: Shows information about the CPU Time Stamp Counter (TSC).
               - deoptimizations-by-reason: Shows the reasons for deoptimizations and their counts.
               - deoptimizations-by-site: Shows the methods where deoptimizations occurred and their counts.
               - events-by-count
               - events-by-name
               - environment-variables
               - exception-count: Shows the total number of exceptions thrown during the JFR recording period.
               - gc: Provides a summary of all garbage collection events, including start time, type, heap usage before and after GC, and the longest pause duration.
               - gc-concurrent-phases: Shows how long each gc phase took on average.
               - gc-parallel-phases: Shows how long each parallel gc phase took on average.
               - gc-configuration: Shows the configuration of the garbage collector (including number of gc threads).
               - gc-references: Shows reference processing statistics during garbage collection including soft, weak, phantom, and final reference counts.
               - gc-pauses: Shows statistics about the garbage collection pauses including total pause time and number of pauses.
               - gc-cpu-time: Summarizes the CPU time consumed by garbage collection.
               - heap-configuration: Displays the configuration settings of the JVM heap, including sizes and compressed oops usage.
               - hot-methods: Identifies the top Java methods where the application spends the most execution time, based on sampling data.
               - jvm-information
               - memory-leaks-by-class
               - memory-leaks-by-site
               - modules
               - native-libraries
               - native-methods: Identifies the top native methods where the application spends the most time, based on sampling data.
               - network-utilization: Displays network utilization statistics for each network interface, including read and write rates.
               - thread-count
               - recording
               - system-properties
               - system-information
               - system-processes
               - thread-allocation
               - thread-cpu-load
               - thread-start
               - vm-operations
               To obtain the sql definition of a view, use 'view_sql("view_name")', to get the columns, use 'DESCRIBE "<view_name>";'
               """)
        String ask(String question);
    }

    private void runWithDB(BasicParallelImporter.CreationResult result, DuckDBConnection conn) {
        QueryExecutor executor = new QueryExecutor(conn);
        System.out.println("Generating LLM description...");
        System.out.println(result.llmDescription());

        List<ToolSpecification> toolSpecifications = ToolSpecifications.toolSpecificationsFrom(new Tools(new QueryExecutor(conn)));
        if (toolSpecifications.isEmpty()) {
            throw new IllegalStateException("No tool specifications found");
        }
        ChatModel model = OllamaChatModel.builder()
                .baseUrl(modelUrl)
                .modelName(this.model.name)
                .temperature(0.0)
                .timeout(Duration.ofMinutes(10))
                .maxRetries(10)
                .build();
        String userMessage = "create root of 3, 5 and 6 and print it as json";

        DBQueryAgent mathGenius = AiServices.builder(DBQueryAgent.class)
                .chatModel(model)
                .tools(new Tools(new QueryExecutor(conn)))
                .build();
        String answer = mathGenius.ask("\n\n----\n\nCreate SQL to find the P99 of garbage collection pause times");

        System.out.println(answer);
    }
}
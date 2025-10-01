package me.bechberger.jfr.duckdb;

import me.bechberger.jfr.duckdb.query.QueryExecutor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Idea: Compare the view output with expected output, uses CSV output and ignores spaces
 */
public class ViewsTest {

    private static final Map<JFRFile, JFRFileHelper> jfrFileHelpers = new HashMap<>();

    private JFRFileHelper getJFRFileHelper(JFRFile jfrFile) throws IOException, SQLException {
        return jfrFileHelpers.computeIfAbsent(jfrFile, jf -> {
            try {
                return new JFRFileHelper(jf);
            } catch (IOException | SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterAll
    public static void cleanup() throws IOException {
        for (JFRFileHelper helper : jfrFileHelpers.values()) {
            helper.cleanup();
        }
    }

    public record ExpectedView(String view, String expectedCSV, JFRFile jfrFile) {
        public ExpectedView(String view, String expectedCSV) {
            this(view, expectedCSV, JFRFile.DEFAULT);
        }
    }

    static Stream<ExpectedView> expectedViews() {
        return Stream.of(
                new ExpectedView("active-recordings",
                        """
                                Start,Duration,Name,Destination,Max Age,Max Size
                                2024-05-24 18:19:43.122,Infinity,1,file.jfr,Infinity,0
                                """),
                new ExpectedView("active-settings",
                        """
                                Event Type,Enabled,Threshold,Stack Trace,Period,Cutoff,Throttle
                                ActiveRecording,true,,,,,
                                ActiveSetting,true,,,,,
                                AllocationRequiringGC,false,,true,,,
                                ...
                                """),
                new ExpectedView("allocation-by-class",
                """
                        Object Type,Allocation Pressure
                        byte[],18.32%
                        scala.collection.immutable.$colon$colon,7.98%
                        java.lang.Object[],6.99%
                        ...
                        """),
                new ExpectedView("allocation-by-thread",
                        """
                                Thread,Allocation Pressure
                                main,99.07%
                                Thread-33,0.03%
                                Thread-35,0.03%
                                Thread-40,0.03%
                                ...
                                """),
                new ExpectedView("allocation-by-site",
                        """
                                Method,Allocation Pressure
                                java.lang.invoke.DirectMethodHandleallocateInstance,6.75%
                                java.util.ArrayscopyOfRange,5.18%
                                dotty.tools.dotc.util.PerfectHashingallocate,3.89%
                                ...
                                """),
                new ExpectedView("class-loaders",
                        """
                                Class Loader,Hidden Classes,Classes
                                java.net.URLClassLoader,0,5338
                                ,334,2755
                                java.net.URLClassLoader,0,572
                                jdk.internal.loader.ClassLoaders$AppClassLoader,0,84
                                jdk.internal.loader.ClassLoaders$PlatformClassLoader,0,7
                                """),
                new ExpectedView("class-modifications",
                        """
                                Time,Requested By,Operation,Classes
                                11.16ms,,Retransform Classes,5
                                """, JFRFile.METAL),
                new ExpectedView("compiler-configuration",
                        """
                                Compiler Threads,Dynamic Compiler Threads,Tiered Compilation
                                4,true,true
                                """),
                new ExpectedView("compiler-statistics",
                        """
                                Compiled Methods,Peak Time,Total Time,Bailouts,OSR Compilations,Standard Compilations,OSR Bytes Compiled,Standard Bytes Compiled,Compilation Resulting Size,Compilation Resulting Code Size
                                30730,591.00ms,70.25s,5,113,30617,0.00 B,0.00 B,135.72 MB,67.83 MB
                                """),
                new ExpectedView("compiler-phases",
                        """
                                Level,Phase,Average,P95,Longest,Count,Total
                                1,Final Code,394.31us,1.34ms,163.73ms,3335,1.32s
                                1,After Parsing,114.55us,545.80us,3.68ms,3360,384.90ms
                                1,Before matching,106.50us,431.90us,7.98ms,3360,357.85ms
                                1,End,14.86us,1.74us,4.90ms,3361,49.95ms
                                ...
                                """,
                        JFRFile.CONTAINER),
                new ExpectedView("container-configuration",
                        """
                                Container Type,CPU Slice Period,CPU Quota,CPU Shares,Effective CPU Count,Memory Soft Limit,Memory Limit,Swap Memory Limit,Host Total Memory
                                cgroupv2,100.00ms,-1.00us,-1,128,0.00 B,-1.00 B,-1.00 B,123.59 GB
                                """, JFRFile.CONTAINER),
                new ExpectedView("container-cpu-usage",
                        """
                                CPU Time,CPU User Time,CPU System Time
                                60.79s,57.41s,3.38s
                                """, JFRFile.CONTAINER),
                new ExpectedView("container-memory-usage",
                        """
                                Memory Fail Count,Memory Usage,Swap Memory Usage
                                0,950.74 MB,950.53 MB
                                """, JFRFile.CONTAINER),
                new ExpectedView("container-io-usage",
                        """
                                Service Requests,Data Transferred
                                2,8.00 KB
                                """, JFRFile.CONTAINER),
                new ExpectedView("container-cpu-throttling",
                        """
                                CPU Elapsed Slices,CPU Throttled Slices,CPU Throttled Time
                                0,0,0s
                                """, JFRFile.CONTAINER),
                new ExpectedView("contention-by-thread",
                        """
                                Thread,Count,Avg,P90,Max.
                                JFR Periodic Tasks,1,77.71ms,77.71ms,77.71ms
                                main,1,37.07us,37.07us,37.07us
                                """, JFRFile.CONTAINER),
                new ExpectedView("contention-by-class",
                        """
                                Lock Class,Count,Avg.,P90,Max.
                                java.lang.ClassValue$ClassValueMap,2185,22.83ms,60.14ms,178.22ms
                                edu.rice.habanero.actors.AkkaActorState$actorLatch$,19583,14.63ms,18.42ms,142.83ms
                                java.util.concurrent.ConcurrentHashMap$ReservationNode,2,77.79ms,101.38ms,107.28ms
                                int[],7,17.78ms,24.09ms,24.10ms
                                org.neo4j.cypher.internal.util.symbols.NodeType,3,13.10ms,13.13ms,13.13ms
                                java.lang.Object,12,11.07ms,11.77ms,11.78ms
                                net.openhft.chronicle.hash.impl.PersistedChronicleHashResources,3,11.07ms,11.21ms,11.25ms
                                """, JFRFile.METAL),
                new ExpectedView("contention-by-site",
                        """
                                StackTrace,Count,Avg.,Max.
                                java.lang.ClassValue$ClassValueMap.associateAccess,8,162.09ms,178.22ms
                                java.lang.ClassValue$ClassValueMap.readAccess,2172,21.99ms,178.19ms
                                java.lang.ClassValue$ClassValueMap.removeAccess,5,163.59ms,177.64ms
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("contention-by-address", // maybe check why if differs from JFR
                        """
                                Monitor Address,Class,Threads,Max Duration
                                0x76a568052470,java.lang.ClassValue$ClassValueMap,254,178.22ms
                                0x76ab624a2620,edu.rice.habanero.actors.AkkaActorState$actorLatch$,1903,142.83ms
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("cpu-information",
                        """
                                CPU,Sockets,Cores,Hardware Threads,Description
                                AMD Zen (HT) SSE SSE2 SSE3 SSSE3 SSE4.1 SSE4.2 SSE4A AMD64,1,128,128,Brand: AMD Ryzen Threadripper PRO 3995WX 64-Cores     \\, Vendor: AuthenticAMD\\nFamily: Zen (0x17)\\, Model: <unknown> (0x31)\\, Stepping: 0x0\\nExt. family: 0x8\\, Ext. model: 0x3\\, Type: 0x0\\, Signature: 0x00830f10\\nFeatures: ebx: 0x10800800\\, ecx: 0x7ef8320b\\, edx: 0x178bfbff\\nExt. features: eax: 0x00830f10\\, ebx: 0x40000000\\, ecx: 0x75c237ff\\, edx: 0x2fd3fbff\\nSupports: On-Chip FPU\\, Virtual Mode Extensions\\, Debugging Extensions\\, Page Size Extensions\\, Time Stamp Counter\\, Model Specific Registers\\, Physical Address Extension\\, Machine Check Exceptions\\, CMPXCHG8B Instruction\\, On-Chip APIC\\, Fast System Call\\, Memory Type Range Registers\\, Page Global Enable\\, Machine Check Architecture\\, Conditional Mov Instruction\\, Page Attribute Table\\, 36-bit Page Size Extension\\, CLFLUSH Instruction\\, Intel Architecture MMX Technology\\, Fast Float Point Save and Restore\\, Streaming SIMD extensions\\, Streaming SIMD extensions 2\\, Hyper Threading\\, Streaming SIMD Extensions 3\\, PCLMULQDQ\\, MONITOR/MWAIT instructions\\, Supplemental Streaming SIMD Extensions 3\\, Fused Multiply-Add\\, CMPXCHG16B\\, Streaming SIMD extensions 4.1\\, Streaming SIMD extensions 4.2\\, x2APIC\\, MOVBE\\, Popcount instruction\\, AESNI\\, XSAVE\\, OSXSAVE\\, AVX\\, F16C\\, LAHF/SAHF instruction support\\, Core multi-processor legacy mode\\, Advanced Bit Manipulations: LZCNT\\, SSE4A: MOVNTSS\\, MOVNTSD\\, EXTRQ\\, INSERTQ\\, Misaligned SSE mode\\, SYSCALL/SYSRET\\, Execute Disable Bit\\, RDTSCP\\, Intel 64 Architecture\\, Invariant TSC
                                """,
                        JFRFile.CONTAINER),
                new ExpectedView("cpu-load",
                        """
                                JVM User (Minimum),JVM User (Average),JVM User (Maximum),JVM System (Minimum),JVM System (Average),JVM System (Maximum),Machine Total (Minimum),Machine Total (Average),Machine Total (Maximum)
                                0.36%,3.85%,8.33%,0.03%,0.86%,8.33%,0.95%,5.03%,16.67%
                                ...
                                """,
                        JFRFile.CONTAINER),
                new ExpectedView("cpu-load-samples",
                        """
                                Time,JVM User,JVM System,Machine Total
                                2025-09-29 17:44:44.277777,8.33%,8.33%,16.67%
                                2025-09-29 17:44:47.332668,1.88%,0.17%,2.30%
                                2025-09-29 17:44:48.208431,0.87%,0.04%,2.40%
                                ...
                                """,
                        JFRFile.CONTAINER),
                // cpu-tsc, deoptimizations-by-reason with metal
                new ExpectedView("cpu-tsc",
                        """
                                Fast Time Auto Enabled,Fast Time Enabled,Fast Time Frequency,OS Frequency
                                true,false,1000000000 Hz,1000000000 Hz
                                """, JFRFile.METAL),
                new ExpectedView("deoptimizations-by-reason",
                        """
                                Reason,Count
                                unstable_if,1912
                                class_check,1402
                                bimorphic_or_optimized_type_check,680
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("deoptimizations-by-site",
                        """
                                Method,Line Number,BCI,Count
                                java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.awaitNanos(J)J,1798,80,173
                                org.lmdbjava.bench.MapDb$1.run()V,53,82,107
                                org.lmdbjava.bench.MvStore$1.run()V,63,82,101
                                org.lmdbjava.bench.Chronicle$1.run()V,63,82,98
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("events-by-count",
                        """
                                Event Label,Count
                                GC Phase Parallel,1041435
                                CPU Time Sample,84280
                                Promotion in new PLAB,68621
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("events-by-name",
                        """
                                Event Label,Count
                                Flight Recording,4
                                Recording Setting,1504
                                Boolean Flag,2040
                                CPU Information,4
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("environment-variables",
                        """
                                Key,Value
                                AUTOJUMP_ERROR_PATH,/home/i560383/.local/share/autojump/errors.log
                                AUTOJUMP_SOURCED,1
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("exception-count",
                        """
                                Exceptions Thrown
                                194481
                                """, JFRFile.METAL),
                new ExpectedView("exception-by-type", // TODO: check why different from JFR
                                """
                                Class,Count
                                java.lang.ClassNotFoundException,5235
                                java.lang.NoSuchMethodError,1298
                                java.io.EOFException,934
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("exception-by-message",
                        """
                                Message,Count
                                ,2562
                                serialPersistentFields,54
                                Multiple exceptions,49
                                serialVersionUID,46
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("exception-by-site",
                        """
                                Method,Count
                                jdk.internal.loader.BuiltinClassLoader.loadClass,5158
                                java.lang.invoke.MethodHandleNatives.resolve,1018
                                java.io.ObjectInputStream$BlockDataInputStream.peekByte,931
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("file-reads-by-path",
                        """
                                Path,Reads,Total Read
                                /renaissance.jar,6064,46.67 MB
                                /harness-154444-12576436177401451626/scala-dotty/lib/scala3-compiler_3-3.3.4.jar,4529,6.66 MB
                                ...
                                """, JFRFile.CONTAINER),
                new ExpectedView("file-writes-by-path",
                        """
                                Path,Writes,Total Written
                                harness-154444-12576436177401451626/scala-dotty/lib/scala3-compiler_3-3.3.4.jar,2377,19.11 MB
                                harness-154444-12576436177401451626/scala-dotty/lib/scala-compiler-2.13.15.jar,1445,11.71 MB
                                ...
                                """, JFRFile.CONTAINER),
                /*new ExpectedView("finalizers",
                        """
                                Time,Thread,Class
                                2024-05-24 16:19:43.246,Finalizer,java.lang.ref.Finalizer
                                2024-05-24 16:19:43.246,Finalizer,java.lang.ref.Finalizer
                                2024-05-24 16:19:43.246,Finalizer,java.lang.ref.Finalizer
                                ...
                                """, JFRFile.CONTAINER)*/
                new ExpectedView("gc",
                        """
                                Start,GC ID,GC Name,Heap Before GC,Heap After GC,Longest Pause
                                2025-09-29 18:09:58.034023,1,G1New,49.62 MB,8.79 MB,7.52ms
                                2025-09-29 18:09:58.267352,2,G1New,40.79 MB,14.43 MB,7.37ms
                                2025-09-29 18:09:58.35344,3,G1New,62.43 MB,27.98 MB,18.05ms
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("gc-concurrent-phases",
                        """
                                Name,Average,P95,Longest,Count,Total
                                Concurrent Mark From Roots,26.10ms,63.34ms,63.34ms,18,469.73ms
                                Concurrent Rebuild Remembered Sets and Scrub Regions,19.12ms,119.16ms,119.16ms,18,344.13ms
                                Concurrent Scan Root Regions,10.56ms,42.79ms,42.79ms,18,190.03ms
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("gc-configuration",
                        """
                                Young GC,Old GC,Parallel GC Threads,Concurrent GC Threads,Dynamic GC Threads,Concurrent Explicit GC,Disable Explicit GC,Pause Target,GC Time Ratio
                                G1New,G1Old,83,21,true,false,false,,12
                                """, JFRFile.METAL),
                new ExpectedView("gc-references",
                        """
                                Time,GC ID,Soft Ref.,Weak Ref.,Phantom Ref.,Final Ref.,Total Count
                                2025-09-29 18:09:58.040782,1,0,668,15,0,683
                                2025-09-29 18:09:58.274328,2,0,737,30,0,767
                                2025-09-29 18:09:58.371203,3,0,136,12,0,148
                                2025-09-29 18:09:58.418165,4,0,0,0,0,0
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("gc-pause-phases", // TODO: check why different from JFR
                        """
                                Type,Name,Average,P95,Longest,Count,Total
                                GC Phase Pause,GC Pause,22.80ms,83.79ms,582.47ms,177,4.04s
                                GC Phase Pause,Pause Remark,5.70ms,13.77ms,13.77ms,18,102.61ms
                                GC Phase Pause,Pause Cleanup,23.26us,45.89us,45.89us,17,395.48us
                                GC Phase Pause Level 1,Phase 1: Mark live objects,31.68ms,57.13ms,57.13ms,11,348.51ms
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("gc-pauses",
                        """
                                Total Pause Time,Number of Pauses,Minimum Pause Time,Median Pause Time,Average Pause Time,P90 Pause Time,P95 Pause Time,P99 Pause Time,P99.9% Pause Time,Maximum Pause Time
                                4.14s,212,9.44us,4.50ms,19.52ms,58.56ms,75.21ms,113.08ms,582.47ms,582.47ms
                                """, JFRFile.METAL),
                new ExpectedView("gc-allocation-trigger",
                        """
                                Trigger Method (Non-JDK),Count,Total Requested
                                dotty.tools.dotc.Compiler.frontendPhases,2,824.00 B
                                dotty.tools.dotc.core.Types$ApproximatingTypeMap.derivedAppliedType,1,8.00 MB
                                scala.tools.asm.tree.MethodNode.visitVarInsn,1,1.10 MB
                                scala.collection.immutable.Map$.from,1,773.28 KB
                                ...
                                """, JFRFile.CONTAINER),
                new ExpectedView("gc-cpu-time",
                        """
                                GC User Time,GC System Time,GC Wall Clock Time,Total Time,GC Count
                                70.33s,12.28s,4.13s,87.94s,212
                                """, JFRFile.METAL),
                new ExpectedView("heap-configuration",
                        """
                                Initial Size,Minimum Size,Maximum Size,If Compressed Oops Are Used,Compressed Oops Mode
                                1.94 GB,16.00 MB,29.97 GB,true,Zero based
                                """, JFRFile.METAL),
                new ExpectedView("hot-methods",
                        """
                                Method,Samples,Percent
                                java.util.concurrent.ForkJoinPool.deactivate,1066,8.09%
                                scala.collection.immutable.RedBlackTree$.lookup,695,5.27%
                                akka.actor.dungeon.Children.initChild,678,5.14%
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("jvm-flags",
                        """
                                Name,Value
                                AOTCache,
                                AOTCacheOutput,
                                AOTClassLinking,false
                                AOTConfiguration,
                                AOTInitTestClass,
                                AOTMode,
                                ActiveProcessorCount,-1
                                AdaptiveSizeDecrementScaleFactor,4
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("jvm-information",
                        """
                                PID,VM Start,Name,Version,VM Arguments,Program Arguments
                                169284,2025-09-29 18:09:57.388,OpenJDK 64-Bit Server VM,OpenJDK 64-Bit Server VM (25+36-LTS) for linux-amd64 JRE (25+36-LTS)\\, built on 2025-09-16T13:51:13Z with gcc 13.2.0,-javaagent:file.jfr\\,settings=profile\\,jdk.CPUTimeSample#enabled=true\\,dumponexit=true,/home/i560383/code/experiments/record-all-events/renaissance.jar all -r 1
                                """, JFRFile.METAL),
                new ExpectedView("latencies-by-type",
                        """
                                Event Type,Count,Average,P 99,Longest,Total
                                Thread Park,41446,210.54ms,1.71s,42.87s,8726.14s
                                Java Monitor Enter,21795,15.45ms,139.49ms,178.22ms,336.79s
                                Java Monitor Wait,393,763.49ms,15.29s,60.05s,300.05s
                                Thread Sleep,3556,63.78ms,200.07ms,5.00s,226.82s
                                File Write,330,3.54ms,8.91ms,13.18ms,1.17s
                                File Read,260,1.77ms,4.11ms,4.36ms,459.62ms
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("memory-leaks-by-class", // TODO: check why different from JFR
                                """
                                Alloc. Time,Object Class,Object Age,Heap Usage
                                2025-09-29 18:10:00.240224,byte[],85.75s,91.65 MB
                                2025-09-29 18:10:01.402116,java.lang.Class,84.59s,93.46 MB
                                2025-09-29 18:10:03.988607,java.lang.ClassValue.Entry[],82.00s,462.39 MB
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("memory-leaks-by-site",
                        """
                                Alloc. Time,Application Method,Object Age,Heap Usage
                                2025-09-29 18:10:00.240224,org.renaissance.apache.spark.SparkUtil.setUpSparkContext,85.75s,91.65 MB
                                2025-09-29 18:10:01.402116,org.apache.logging.log4j.spi.AbstractLogger.createDefaultFlowMessageFactory,84.59s,93.46 MB
                                2025-09-29 18:10:03.988607,org.apache.spark.serializer.JavaSerializationStream.writeObject,82.00s,462.39 MB
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("modules",
                        """
                                Module Name
                                java.base
                                java.compiler
                                java.datatransfer
                                java.desktop
                                java.desktop
                                ...
                                """),
                new ExpectedView("monitor-inflation",
                        """
                                Method,Monitor Class,Count,Total Duration
                                java.util.zip.ZipFile$CleanableResource.getInflater,int[],15,16.94us
                                java.util.zip.ZipFile$CleanableResource.getInflater,java.lang.Object,13,8.00us
                                java.lang.Object.wait0,java.lang.Thread,3,3.51us
                                java.lang.Object.wait0,dotty.tools.dotc.util.concurrent$Executor,1,2.00us
                                java.io.PrintStream.write,java.io.PrintStream,1,1.35us
                                java.util.zip.ZipFile$CleanableResource.getInflater,scala.collection.immutable.LazyList,1,1.20us
                                jdk.jfr.internal.PlatformRecorder.periodicTask,jdk.jfr.internal.PlatformRecorder,1,1.00us
                                java.util.zip.ZipFile$CleanableResource.getInflater,scala.collection.mutable.HashMap,1,441.00ns
                                """, JFRFile.CONTAINER),
                new ExpectedView("native-libraries",
                        """
                                Name,Base Address,Top Address
                                /home/i560383/.cache/JNA/temp/jna5078824034297894815.tmp,0x76a0b1600000,0x76a0b1a00000
                                /home/i560383/.cache/JNA/temp/jna7893014146703245882.tmp,0x76a0b2400000,0x76a0b2800000
                                /home/i560383/.sdkman/candidates/java/25-sapmchn/bin/../lib/libjli.so,0x76ab69bc0000,0x76ab69bd1000
                                /home/i560383/.sdkman/candidates/java/25-sapmchn/lib/libextnet.so,0x76ab69ba5000,0x76ab69ba9000
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("native-methods",
                        """
                                Method,Samples,Percent
                                sun.nio.ch.EPoll.wait,863,36.61%
                                sun.nio.fs.LinuxWatchService.poll,693,29.40%
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("network-utilization",
                        """
                                Network Interface,Avg. Read Rate,Max. Read Rate,Avg. Write Rate,Max. Write Rate
                                eth0,239.75 B/s,595.00 B/s,6.75 B/s,27.00 B/s
                                """, JFRFile.CONTAINER),
                new ExpectedView("object-statistics", // TODO: check why different from JFR
                        """
                                Class,Count,Heap Space,Increase
                                java.lang.Object,264572,2.02 MB,1.00 MB
                                byte[],21679,1.26 MB,8.35 MB
                                java.util.zip.ZipEntry,14351,1.09 MB,0.00 B
                                ...
                                """, JFRFile.CONTAINER),
                new ExpectedView("thread-count",
                        """
                                Start Time,Active Threads,Daemon Threads,Accumulated Threads,Peak Threads
                                2025-09-29 18:09:58.820285,19,18,21,20
                                2025-09-29 18:09:59.826868,146,145,148,146
                                2025-09-29 18:10:00.84577,146,145,149,147
                                2025-09-29 18:10:01.85013,202,201,205,202
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("recording",
                        """
                                Event Count,First Recorded Event,Last Recorded Event,Length of Recorded Events,Dump Reason
                                86335,2024-05-24 18:19:43.125774,2024-05-24 18:20:50.589537,19.18s,No remaining non-daemon Java threads
                                """),
                new ExpectedView("longest-compilations",
                        """
                                Start Time,Duration,Method,Compile Level,Succeeded
                                2025-09-29 18:11:06.403713,540.73ms,org.h2.mvstore.MVMap.operate,4,true
                                2025-09-29 18:10:15.084012,481.31ms,akka.actor.ActorCell.invoke,4,true
                                2025-09-29 18:10:15.089539,477.36ms,akka.dispatch.Mailbox.processMailbox,4,true
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("longest-class-loading",
                        """
                                Time,Loaded Class,Load Time
                                2025-09-29 17:44:46.210251,dotty.tools.dotc.transform.Memoize$MyState,449.81ms
                                2025-09-29 17:44:44.917427,scala.collection.immutable.List,23.83ms
                                ...
                                """, JFRFile.CONTAINER),
                new ExpectedView("system-properties",
                        """
                                Key,Value
                                java.class.path,/home/i560383/code/experiments/record-all-events/renaissance.jar
                                java.home,/home/i560383/.sdkman/candidates/java/25-sapmchn
                                java.library.path,/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib
                                java.vm.compressedOopsMode,Zero based
                                java.vm.info,mixed mode\\, sharing
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("system-information",
                        """
                                Total Physical Memory Size,OS Version,CPU Type,Number of Cores,Number of Hardware Threads,Number of Sockets,CPU Description
                                123.59 GB,ID=gardenlinux\\nNAME=""Garden Linux""\\nPRETTY_NAME=""Garden Linux 1592.13""\\nHOME_URL=""https://gardenlinux.io""\\nSUPPORT_URL=""https://github.com/gardenlinux/gardenlinux""\\nBUG_REPORT_URL=""https://github.com/gardenlinux/gardenlinux/issues""\\nGARDENLINUX_CNAME=container-amd64-1592.13\\nGARDENLINUX_FEATURES=_slim\\,base\\,container\\nGARDENLINUX_VERSION=1592.13\\nGARDENLINUX_COMMIT_ID=2b302ec2\\nGARDENLINUX_COMMIT_ID_LONG=2b302ec2abc9275921e1321074b9d6f566860fd1\\nuname: Linux 6.14.0-29-generic #29-Ubuntu SMP PREEMPT_DYNAMIC Thu Aug  7 18:32:38 UTC 2025 x86_64\\nlibc: glibc 2.39 NPTL 2.39 \\n,AMD Zen (HT) SSE SSE2 SSE3 SSSE3 SSE4.1 SSE4.2 SSE4A AMD64,128,128,1,Brand: AMD Ryzen Threadripper PRO 3995WX 64-Cores     \\, Vendor: AuthenticAMD\\nFamily: Zen (0x17)\\, Model: <unknown> (0x31)\\, Stepping: 0x0\\nExt. family: 0x8\\, Ext. model: 0x3\\, Type: 0x0\\, Signature: 0x00830f10\\nFeatures: ebx: 0x10800800\\, ecx: 0x7ef8320b\\, edx: 0x178bfbff\\nExt. features: eax: 0x00830f10\\, ebx: 0x40000000\\, ecx: 0x75c237ff\\, edx: 0x2fd3fbff\\nSupports: On-Chip FPU\\, Virtual Mode Extensions\\, Debugging Extensions\\, Page Size Extensions\\, Time Stamp Counter\\, Model Specific Registers\\, Physical Address Extension\\, Machine Check Exceptions\\, CMPXCHG8B Instruction\\, On-Chip APIC\\, Fast System Call\\, Memory Type Range Registers\\, Page Global Enable\\, Machine Check Architecture\\, Conditional Mov Instruction\\, Page Attribute Table\\, 36-bit Page Size Extension\\, CLFLUSH Instruction\\, Intel Architecture MMX Technology\\, Fast Float Point Save and Restore\\, Streaming SIMD extensions\\, Streaming SIMD extensions 2\\, Hyper Threading\\, Streaming SIMD Extensions 3\\, PCLMULQDQ\\, MONITOR/MWAIT instructions\\, Supplemental Streaming SIMD Extensions 3\\, Fused Multiply-Add\\, CMPXCHG16B\\, Streaming SIMD extensions 4.1\\, Streaming SIMD extensions 4.2\\, x2APIC\\, MOVBE\\, Popcount instruction\\, AESNI\\, XSAVE\\, OSXSAVE\\, AVX\\, F16C\\, LAHF/SAHF instruction support\\, Core multi-processor legacy mode\\, Advanced Bit Manipulations: LZCNT\\, SSE4A: MOVNTSS\\, MOVNTSD\\, EXTRQ\\, INSERTQ\\, Misaligned SSE mode\\, SYSCALL/SYSRET\\, Execute Disable Bit\\, RDTSCP\\, Intel 64 Architecture\\, Invariant TSC
                                """, JFRFile.CONTAINER),
                new ExpectedView("system-processes",
                        """
                                First Observed,Last Observed,PID,Command Line
                                2025-09-29 17:44:47.331816,2025-09-29 17:44:47.331816,1,java -agentlib:jdwp=transport=dt_socket\\,server=y\\,suspend=n\\,address=5005 -javaagent:file.jfr\\,settings=/all.jfc\\,jdk.CPUTimeSample#enabled=true\\,dumponexit=true -jar /renaissance.jar dotty -r 1
                                """, JFRFile.CONTAINER),
                new ExpectedView("tlabs",
                        """
                                Inside Count,Inside Minimum Size,Inside Average Size,Inside Maximum Size,Inside Total Allocation,Outside Count,Outside Minimum Size,Outside Average Size,Outside Maximum Size,Outside Total Allocation
                                15952,2.57 KB,30.29 KB,8.00 MB,471.82 MB,7467,56.00 B,6.53 KB,1.00 MB,47.62 MB
                                """, JFRFile.CONTAINER),
                new ExpectedView("thread-cpu-load",
                        """
                                Thread,System,User
                                Thread-767,0.00%,0.78%
                                Thread-732,0.00%,0.78%
                                Thread-776,0.00%,0.78%
                                ...
                                """, JFRFile.METAL),
                new ExpectedView("thread-start",
                        """
                                Start Time,Stack Trace,Thread,Duration
                                2025-09-29 17:44:44.317597,,Notification Thread,infinity
                                2025-09-29 17:44:56.102034,,DestroyJavaVM,infinity
                                2025-09-29 17:44:56.1025,java.lang.ApplicationShutdownHooks.runHooks,Thread-2,infinity
                                2025-09-29 17:44:56.102635,java.lang.ApplicationShutdownHooks.runHooks,JFR Shutdown Hook,infinity
                                2025-09-29 17:44:44.317596,,main,11.78s
                                ...
                                """, JFRFile.CONTAINER),
                new ExpectedView("vm-operations",
                        """
                                VM Operation,Average Duration,Longest Duration,Count,Total Duration
                                G1CollectForAllocation,19.87ms,582.59ms,158,3.14s
                                G1CollectFull,76.60ms,113.28ms,11,842.63ms
                                G1PauseRemark,5.86ms,13.97ms,18,105.53ms
                                ...
                                """, JFRFile.METAL)
        );
    }

    private String normalizeCSV(String csv) {
        return csv.lines().limit(100).collect(Collectors.joining("\n"))
                .replaceAll("/.+/.+\\.jfr", "file.jfr")
                .replace("\r\n", "\n").trim();
    }

    @ParameterizedTest
    @MethodSource("expectedViews")
    public void testViewOutput(ExpectedView expectedView) throws SQLException, IOException, InterruptedException {
        var out = new ByteArrayOutputStream();
        var printStream = new PrintStream(new BufferedOutputStream(out));
        var executor = new QueryExecutor(getJFRFileHelper(expectedView.jfrFile).openConnection());
        executor.executeQuery("SELECT * FROM \"" + expectedView.view + "\"", QueryExecutor.OutputFormat.CSV, printStream);
        printStream.flush();
        String result = normalizeCSV(out.toString());
        String expected = normalizeCSV(expectedView.expectedCSV);
        System.out.println(expectedView.view + ":\nExpected:\n" + expected + "\nGot:\n" + result);
        System.out.println("---");
        System.out.println(getJFRFileHelper(expectedView.jfrFile).executeJFR(expectedView.view));
        if (expected.endsWith("...")) {
            expected = expected.substring(0, expected.length() - 3).trim();
            result = result.length() > expected.length() ? result.substring(0, expected.length()) : result;
        }
        Assertions.assertEquals(expected, result, "View output does not match for view: " + expectedView.view);
    }
}
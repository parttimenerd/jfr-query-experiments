
                                                  Monitor Inflation

Stack Trace                                           Monitor Class                             Count Total Duration
----------------------------------------------------- ----------------------------------------- ----- --------------
java.lang.Object.wait0(long)                          java.lang.Thread                              2     0,00255 ms
java.util.zip.ZipFile$CleanableResource.getInflater() int[]                                         1     0,00228 ms
java.lang.Object.wait0(long)                          dotty.tools.dotc.util.concurrent$Executor     1     0,00200 ms
java.util.zip.ZipFile$CleanableResource.getInflater() int[]                                         2     0,00195 ms
java.util.zip.ZipFile$CleanableResource.getInflater() int[]                                         1     0,00154 ms
java.util.zip.ZipFile$CleanableResource.getInflater() int[]                                         1     0,00142 ms
java.util.zip.ZipFile$CleanableResource.getInflater() int[]                                         1     0,0013


the method getInflater is coming up multiple times, possibly because of chunks
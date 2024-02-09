using System.Diagnostics;
using System.IO.MemoryMappedFiles;

namespace OneBillionRows;

public class Program
{
    private const long WindowsSize = 32 * 1024 * 1024;
    private const long BufferSize = 2 * 1024;
    private readonly CountdownEvent _cde;

    private readonly long _fileSize;
    private readonly MemoryMappedFile _mmf;
    private int _results;
    private long _section = -1;

    private Program(MemoryMappedFile mmf, long fileSize, int threadCount)
    {
        _mmf = mmf;
        _fileSize = fileSize;
        _cde = new CountdownEvent(threadCount);
    }

    private long Results => _results;

    public void Wait()
    {
        _cde.Wait();
    }

    public bool Signal()
    {
        return _cde.Signal();
    }

    public static void Main()
    {
        var path = "C:\\Users\\Mats Fredriksson\\Documents\\Code\\1brc\\measurements.txt";


        var fileSize = new FileInfo(path).Length;
        var stopWatch = new Stopwatch();
        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var tasks = new List<Task>();
            var cores = Environment.ProcessorCount;
            var processor = new Program(mmf, fileSize, cores);
            Console.WriteLine($"Processors: {cores}");
            stopWatch.Start();
#if false
            for (var ctr = 0; ctr < cores; ++ctr)
                tasks.Add(Task.Run(() => processor.ProcessUnsafe()));
            Task.WaitAll(tasks.ToArray());
#else
            for (var ctr = 0; ctr < cores; ++ctr) ThreadPool.QueueUserWorkItem(ThreadMain, processor);
            processor.Wait();
#endif
            stopWatch.Stop();
            Console.WriteLine($"Runtime: {stopWatch}");

            Console.WriteLine($"Lines: {processor.Results}");
        }
    }

    public static void ThreadMain(object state)
    {
        var proc = (Program)state;
        proc.ProcessSafe();
        proc.Signal();
    }

    private void ProcessUnsafe()
    {
        while (true)
        {
            var lines = 0;
            var mySection = Interlocked.Increment(ref _section);
            var start = mySection * WindowsSize;
            if (start >= _fileSize)
                return;

            var length = WindowsSize;
            if (start + WindowsSize > _fileSize)
                length = 0;
            using (var accessor = _mmf.CreateViewAccessor(start, length))
            {
                using (var handle = accessor.SafeMemoryMappedViewHandle)
                {
                    unsafe
                    {
                        byte* pStart = null;
                        handle.AcquirePointer(ref pStart);
                        var len = handle.ByteLength;
                        byte current = 0;
                        for (ulong i = 0; i < len; ++i)
                        {
                            current = *(pStart + i);
                            lines += current == '\n' ? 1 : 0;
                        }

                        handle.ReleasePointer();
                    }
                }
            }

            Interlocked.Add(ref _results, lines);
        }
    }

    public void ProcessSafe()
    {
        var span = new Span<byte>(new byte[BufferSize]);
        while (true)
        {
            var lines = 0;
            var mySection = Interlocked.Increment(ref _section);
            var start = mySection * WindowsSize;
            if (start >= _fileSize)
                return;

            var length = WindowsSize;
            if (start + WindowsSize > _fileSize)
                length = 0;
            using (var accessor = _mmf.CreateViewAccessor(start, length))
            {
                var offset = 0L;
                using (var handle = accessor.SafeMemoryMappedViewHandle)
                {
                    while (offset + BufferSize < accessor.Capacity)
                    {
                        span.Clear();
                        handle.ReadSpan((ulong)offset, span);
                        for (var i = 0; i < span.Length; ++i)
                            lines += span[i] == '\n' ? 1 : 0;
                        offset += BufferSize;
                    }

                    var slice = span.Slice(0, (int)(accessor.Capacity - offset));
                    handle.ReadSpan((ulong)offset, slice);
                    for (var i = 0; i < slice.Length; ++i)
                        lines += slice[i] == '\n' ? 1 : 0;
                }
            }

            Interlocked.Add(ref _results, lines);
        }
    }
}
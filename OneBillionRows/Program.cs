using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Text;

namespace OneBillionRows;

public class Program
{
    private const long WindowsSize = 32 * 1024 * 1024;

//    private const long WindowsSize = 8 * 1024;
    private const long Overlap = 100 + 1 + 5;
    private readonly CountdownEvent _cde;

    private readonly long _fileSize;

    private readonly ConcurrentDictionary<string, ConcurrentBag<float>> _items = new();
    private readonly MemoryMappedFile _mmf;
    private readonly Dictionary<string, BagItem> _result = new();

    private readonly int _simdLength = Vector<float>.Count;
    private int _allLines;
    private long _section = -1;


    private Program(MemoryMappedFile mmf, long fileSize, int threadCount)
    {
        _mmf = mmf;
        _fileSize = fileSize;
        _cde = new CountdownEvent(threadCount);
    }

    private long Count => _allLines;

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
        var path = "C:\\Users\\Mats Fredriksson\\Documents\\Code\\1brc\\measurements_1GB.txt";


        var fileSize = new FileInfo(path).Length;
        var stopWatch = new Stopwatch();
        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var tasks = new List<Task>();
            var cores = Environment.ProcessorCount;
            var processor = new Program(mmf, fileSize, cores);
            Console.WriteLine($"Processors: {cores}");
            stopWatch.Start();
#if true
            for (var ctr = 0; ctr < cores; ++ctr)
                tasks.Add(Task.Run(() => processor.Process()));
            Task.WaitAll(tasks.ToArray());
#else
            for (var ctr = 0; ctr < cores; ++ctr) ThreadPool.QueueUserWorkItem(ThreadMain, processor);
            processor.Wait();
#endif
            stopWatch.Stop();
            Console.WriteLine($"Runtime: {stopWatch}");

            Console.WriteLine($"Lines: {processor.Count}");
        }
    }

    public static void ThreadMain(object? state)
    {
        var proc = (Program?)state;
        proc?.Process();
        proc?.Signal();
    }

    private void Process()
    {
        var dict = new Dictionary<string, BagItem>();
        var stationBuffer = new byte[100];
        var stationSpan = new Span<byte>(stationBuffer);
        var numberSpan = new Span<byte>(new byte[16]);
        while (true)
        {
            var lines = 0;
            var mySection = Interlocked.Increment(ref _section);
            var start = mySection * WindowsSize;
            if (start >= _fileSize)
                return;

            var length = WindowsSize + Overlap;
            if (start + length > _fileSize)
                length = 0;
            using (var accessor = _mmf.CreateViewAccessor(start, length))
            {
                using (var handle = accessor.SafeMemoryMappedViewHandle)
                {
                    unsafe
                    {
                        byte* pStart = null;
                        handle.AcquirePointer(ref pStart);
                        pStart += accessor.PointerOffset;
                        var remaining = (ulong)(_fileSize - start);
                        var len = Math.Min(WindowsSize, remaining);
                        var pEnd = pStart + len;
                        var pCurr = pStart;

                        if (start != 0)
                        {
                            while (*pCurr != '\n')
                                ++pCurr;
                            ++pCurr;
                        }

                        if (length != 0)
                            while (*pEnd != '\n')
                                ++pEnd;

                        while (pCurr < pEnd && *pCurr != 0)
                        {
                            if (lines == 1000000 - 1)
                                lines = 1000000 - 1;
                            stationSpan.Clear();
                            var tIdx = 0;
                            while (*pCurr != ';')
                                stationSpan[tIdx++] = *pCurr++;

                            ++pCurr; // ;
                            numberSpan.Clear();
                            var nIdx = 0;
                            while (*pCurr != '\n')
                                numberSpan[nIdx++] = *pCurr++;

                            ++pCurr; // \n

                            var station = Encoding.UTF8.GetString(stationSpan.Slice(0, tIdx));
                            if (!dict.TryGetValue(station, out var bag))
                            {
                                bag = new BagItem();
                                dict.Add(station, bag);
                            }

                            var value = float.Parse(numberSpan.Slice(0, nIdx), CultureInfo.InvariantCulture);
                            bag.Add(value);
                            lines++;
                        }

                        handle.ReleasePointer();
                    }
                }
            }

            Interlocked.Add(ref _allLines, lines);
        }
    }

    public string GetResult()
    {
        var outputList =
            _result
                .OrderBy(kvp => kvp.Key)
                .Select(kvp =>
                {
                    var min = kvp.Value.Min();
                    var avg = kvp.Value.Avg();
                    var max = kvp.Value.Max();
                    var result = string.Create(CultureInfo.InvariantCulture,
                        $"{kvp.Key}={min:F1}/{avg:F1}/{max:F1}");
                    return result;
                })
                .ToList();
        var output = string.Join(", ", outputList);
        return output;
    }


    private class BagItem
    {
        private readonly float[] _items = new float[Vector<float>.Count];
        private int _count;
        public Vector<float> MaxVector { get; set; } = new(float.MinValue);
        public Vector<float> MinVector { get; set; } = new(float.MaxValue);
        public Vector<float> TotalVector { get; set; } = Vector<float>.Zero;

        public void Add(float value)
        {
            _items[_count++ % Vector<float>.Count] = value;

            if (_count % Vector<float>.Count == 0)
            {
                var va = new Vector<float>(_items);
                MaxVector = Vector.Max(MaxVector, va);
                MinVector = Vector.Min(MinVector, va);
                TotalVector = Vector.Add(TotalVector, va);
            }
        }

        public float Max()
        {
            var max = float.MinValue;
            for (var i = 0; i < _count % Vector<float>.Count; ++i)
                max = Math.Max(max, _items[i]);
            for (var i = 0; i < Vector<float>.Count; ++i)
                max = Math.Max(max, MaxVector[i]);
            return max;
        }

        public float Min()
        {
            var min = float.MaxValue;
            for (var i = 0; i < _count % Vector<float>.Count; ++i)
                min = Math.Min(min, _items[i]);
            for (var i = 0; i < Vector<float>.Count; ++i)
                min = Math.Min(min, MinVector[i]);
            return min;
        }

        public float Avg()
        {
            var sum = Vector.Sum(TotalVector);
            for (var i = 0; i < _count % Vector<float>.Count; ++i)
                sum += _items[i];

            return sum / _count;
        }
    }
}
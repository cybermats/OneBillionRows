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

    private const long Overlap = 100 + 1 + 5;

    private readonly ConcurrentDictionary<byte[], BagItem> _aggregate = new(new BagKeyComparer());

    private readonly long _fileSize;

    private readonly MemoryMappedFile _mmf;

    private int _allLines;
    private long _section = -1;


    private Program(MemoryMappedFile mmf, long fileSize)
    {
        _mmf = mmf;
        _fileSize = fileSize;
    }

    private long Count => _allLines;

    public static void Main()
    {
        var path = "C:\\Users\\Mats Fredriksson\\Documents\\Code\\1brc\\measurements.txt";


        var fileSize = new FileInfo(path).Length;
        var stopWatch = new Stopwatch();
        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var tasks = new List<Task>();
            var cores = Environment.ProcessorCount;
#if (DEBUG)
            cores = 1;
#endif

            var processor = new Program(mmf, fileSize);
            Console.WriteLine($"Processors: {cores}");
            stopWatch.Start();
            for (var ctr = 0; ctr < cores; ++ctr)
                tasks.Add(Task.Run(() => processor.Process()));
            Task.WaitAll(tasks.ToArray());
            var output = processor.GetResult();
            File.WriteAllText(@"C:\Users\Mats Fredriksson\Documents\Code\OneBillionRows\OneBillionRows\Output\obr.txt",
                output);
            stopWatch.Stop();
            Console.WriteLine($"Runtime: {stopWatch}");

            Console.WriteLine($"Lines: {processor.Count}");
        }
    }

    private void Process()
    {
        var dict = new Dictionary<byte[], BagItem>(new BagKeyComparer());
        var stationBuffer = new byte[100];
        var stationSpan = new Span<byte>(stationBuffer);
        var numberSpan = new Span<byte>(new byte[16]);
        while (true)
        {
            var lines = 0;
            var mySection = Interlocked.Increment(ref _section);
            var start = mySection * WindowsSize;
            if (start >= _fileSize)
                break;

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

                            if (!dict.TryGetValue(stationBuffer, out var bag))
                            {
                                bag = new BagItem();
                                var arr = stationSpan.Slice(0, tIdx).ToArray();
                                dict.Add(arr, bag);
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

        foreach (var kvp in dict)
        {
            var value = _aggregate.GetOrAdd(kvp.Key, _ => new BagItem());
            lock (value)
            {
                value.Append(kvp.Value);
            }
        }
    }

    public string GetResult()
    {
        var outputList =
            _aggregate
                .Select(kvp =>
                {
                    var station = Encoding.UTF8.GetString(kvp.Key);
                    var min = kvp.Value.Min();
                    var avg = kvp.Value.Avg();
                    var max = kvp.Value.Max();
                    var result = string.Create(CultureInfo.InvariantCulture,
                        $"{station}={min:F1}/{avg:F1}/{max:F1}");
                    return result;
                })
                .OrderBy(i => i, StringComparer.Ordinal)
                .ToList();
        var output = string.Join(", ", outputList);
        return $"{{{output}}}";
    }

    private class BagKeyComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[]? b1, byte[]? b2)
        {
            if (ReferenceEquals(b1, b2))
                return true;

            if (b2 is null || b1 is null)
                return false;

            var len = Math.Min(b1.Length, b2.Length);
            for (var i = 0; i < len; ++i)
                if (b1[i] != b2[i])
                    return false;
            return true;
        }

        public int GetHashCode(byte[] b)
        {
            int hash = b[0];
            var len = Math.Min(b.Length, 4);
            for (var i = 1; i < len && b[i] != 0; i++) hash = (hash << 8) | b[i];
            return hash;
        }
    }

    private class BagItemSSE
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

        public void Append(BagItemSSE other)
        {
            MaxVector = Vector.Max(MaxVector, other.MaxVector);
            MinVector = Vector.Min(MinVector, other.MinVector);
            TotalVector = Vector.Add(TotalVector, other.TotalVector);

            _count += other._count - other._count % Vector<float>.Count;

            for (var i = 0; i < other._count % Vector<float>.Count; ++i)
                Add(other._items[i]);
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

    private class BagItem
    {
        private int _count;
        private float _max = float.MinValue;
        private float _min = float.MaxValue;
        private float _total;

        public void Add(float value)
        {
            _max = Math.Max(_max, value);
            _min = Math.Min(_min, value);
            _total += value;
            _count++;
        }

        public void Append(BagItem other)
        {
            _max = Math.Max(_max, other._max);
            _min = Math.Min(_min, other._min);
            _total += other._total;
            _count += other._count;
        }

        public float Max()
        {
            return _max;
        }

        public float Min()
        {
            return _min;
        }

        public float Avg()
        {
            return _total / _count;
        }
    }
}
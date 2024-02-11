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

    private readonly long _fileSize;

    private readonly MemoryMappedFile _mmf;

    private readonly Dictionary<BagKey, BagItem> _result = new();

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
        Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
        var path = "C:\\Users\\Mats Fredriksson\\Documents\\Code\\1brc\\measurements.txt";


        var fileSize = new FileInfo(path).Length;
        var stopWatch = new Stopwatch();
        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var tasks = new List<Task>();
            var cores = Environment.ProcessorCount;
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
        var dict = new Dictionary<BagKey, BagItem>();
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

                            var key = new BagKey(stationBuffer, tIdx); // 10 sec
                            if (!dict.TryGetValue(key, out var bag))
                            {
                                bag = new BagItem();
                                key.Initialize();
                                dict.Add(key, bag);
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

        lock (_result)
        {
            foreach (var kvp in dict)
                if (_result.TryGetValue(kvp.Key, out var bag))
                {
                    bag.Append(kvp.Value);
                }
                else
                {
                    bag = kvp.Value;
                    _result.Add(kvp.Key, bag);
                }
        }
    }

    public string GetResult()
    {
        var outputList =
            _result
                .Select(kvp =>
                {
                    var station = kvp.Key.ToString();
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

    private class BagKey : IEquatable<BagKey>
    {
        private readonly int _length;
        private byte[] _buffer;

        public BagKey(byte[] buffer, int length)
        {
            _buffer = buffer;
            _length = length;
        }


        public bool Equals(BagKey? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (_length != other._length) return false;
            for (var i = 0; i < _length; ++i)
                if (_buffer[i] != other._buffer[i])
                    return false;
            return true;
        }

        public void Initialize()
        {
            _buffer = _buffer.ToArray();
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((BagKey)obj);
        }

        public override int GetHashCode()
        {
            return _buffer[0] | (_buffer[1] << 8) | (_buffer[2] << 16) | (_buffer[3] << 24);
        }


        public override string ToString()
        {
            return Encoding.UTF8.GetString(_buffer, 0, _length);
        }
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

        public void Append(BagItem other)
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
}
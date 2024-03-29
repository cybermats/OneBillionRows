﻿using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
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

                            if (!dict.TryGetValue(stationBuffer, out var bag))
                            {
                                bag = new BagItem();
                                var arr = stationSpan.Slice(0, tIdx).ToArray();
                                dict.Add(arr, bag);
                            }

                            ++pCurr; // ;

                            var sign = 1;
                            var value = *pCurr++ - '0';
                            if (value < 0)
                            {
                                sign = -1;
                                value = 0;
                            }

                            int digit;
                            while ((digit = *pCurr++ - '0') >= 0) value = value * 10 + digit;
                            value = value * 10 + (*pCurr++ - '0');

                            ++pCurr; // \n

                            bag.Add(sign * value);
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
            var len = Math.Min(b1!.Length, b2!.Length);
            for (var i = 0; i < len; ++i)
                if (b1[i] != b2[i])
                    return false;
            return true;
        }

        public int GetHashCode(byte[] b)
        {
            int hash = b[0];
            if (b.Length >= 2)
                hash |= b[1] << 8;
            if (b.Length >= 3)
                hash |= b[2] << 16;
            if (b.Length >= 4)
                hash |= b[3] << 24;

            return hash;
        }
    }

    private class BagItem
    {
        private int _count;
        private int _max = int.MinValue;
        private int _min = int.MaxValue;
        private int _total;

        public void Add(int value)
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
            return _max / 10.0f;
        }

        public float Min()
        {
            return _min / 10.0f;
        }

        public float Avg()
        {
            return _total / (10.0f * _count);
        }
    }
}
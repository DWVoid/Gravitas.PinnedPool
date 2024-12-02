using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Gravitas.PinnedPool;

namespace Benchmark;

public class Benchmarks
{
    private const int Threads = 16;
    private const int Iterations = 64 * 1024;
    private const int ArraySize = 1024;
    private static readonly PinnedArrayPool<byte> Pool = PinnedArrayPool<byte>.Instance;

    [Benchmark]
    public void ArrayPoolConcurrent()
    {
        var tasks = new Task[Threads];
        for (var i = 0; i < Threads; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                for (var j = 0; j < Iterations; j++)
                {
                    var arr = Pool.Rent(ArraySize);
                    // emulation of O(ArraySize) workload
                    // to add a reasonable delay between .Rent and .Return
                    Random.Shared.NextBytes(arr);
                    Pool.Return(arr);
                }
            });
        }

        Task.WaitAll(tasks);
    }

    [Benchmark]
    public void ArrayPoolConcurrent_TwoArrays()
    {
        var tasks = new Task[Threads];
        for (var i = 0; i < Threads; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                for (var j = 0; j < Iterations; j++)
                {
                    var arr1 = Pool.Rent(ArraySize);
                    var arr2 = Pool.Rent(ArraySize);
                    Random.Shared.NextBytes(arr1);
                    Random.Shared.NextBytes(arr2);
                    Pool.Return(arr2);
                    Pool.Return(arr1);
                }
            });
        }

        Task.WaitAll(tasks);
    }
}
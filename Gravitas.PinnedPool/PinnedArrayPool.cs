// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Gravitas.PinnedPool;

/// <summary>
/// Provides an ArrayPool implementation meant to be used as the singleton returned from ArrayPool.Shared.
/// </summary>
/// <remarks>
/// The implementation uses a tiered caching scheme, with a small per-thread cache for each array size, followed
/// by a cache per array size shared by all threads, split into per-core stacks meant to be used by threads
/// running on that core.  Locks are used to protect each per-core stack, because a thread can migrate after
/// checking its processor number, because multiple threads could interleave on the same core, and because
/// a thread is allowed to check other core's buckets if its core's bucket is empty/full.
/// </remarks>
public sealed class PinnedArrayPool<T> : ArrayPool<T> where T : unmanaged
{
    private PinnedArrayPool() { }

    public static readonly PinnedArrayPool<T> Instance = new();
    
    /// <summary>The number of buckets (array sizes) in the pool, one for each array length, starting from length 16.</summary>
    private const int NumBuckets = 27; // Utilities.SelectBucketIndex(1024 * 1024 * 1024 + 1)

    /// <summary>A per-thread array of arrays, to cache one array per array size per thread.</summary>
    [ThreadStatic] private static SharedArrayPoolThreadLocalArray[]? t_tlsBuckets;

    /// <summary>Used to keep track of all thread local buckets for trimming if needed.</summary>
    private readonly ConditionalWeakTable<SharedArrayPoolThreadLocalArray[], object?> _allTlsBuckets = new();

    /// <summary>
    /// An array of per-core partitions. The slots are lazily initialized to avoid creating
    /// lots of overhead for unused array sizes.
    /// </summary>
    private readonly SharedArrayPoolPartitions?[] _buckets = new SharedArrayPoolPartitions[NumBuckets];

    /// <summary>Whether the callback to trim arrays in response to memory pressure has been created.</summary>
    private int _trimCallbackCreated;

    /// <summary>Allocate a new <see cref="SharedArrayPoolPartitions"/> and try to store it into the <see cref="_buckets"/> array.</summary>
    private SharedArrayPoolPartitions CreatePerCorePartitions(int bucketIndex)
    {
        var inst = new SharedArrayPoolPartitions();
        return Interlocked.CompareExchange(ref _buckets[bucketIndex], inst, null) ?? inst;
    }

    /// <summary>Gets an ID for the pool to use with events.</summary>
    private int Id => GetHashCode();

    public override T[] Rent(int minimumLength)
    {
        T[]? buffer;

        // Get the bucket number for the array length. The result may be out of range of buckets,
        // either for too large a value or for 0 and negative values.
        var bucketIndex = Utilities.SelectBucketIndex(minimumLength);

        // First, try to get an array from TLS if possible.
        var tlsBuckets = t_tlsBuckets;
        if (tlsBuckets is not null && (uint)bucketIndex < (uint)tlsBuckets.Length)
        {
            buffer = Unsafe.As<T[]?>(tlsBuckets[bucketIndex].Array);
            if (buffer is not null)
            {
                tlsBuckets[bucketIndex].Array = null;
                return buffer;
            }
        }

        // Next, try to get an array from one of the partitions.
        var perCoreBuckets = _buckets;
        if ((uint)bucketIndex < (uint)perCoreBuckets.Length)
        {
            var b = perCoreBuckets[bucketIndex];
            if (b is not null)
            {
                buffer = Unsafe.As<T[]?>(b.TryPop());
                if (buffer is not null) return buffer;
            }

            // No buffer available.  Ensure the length we'll allocate matches that of a bucket
            // so we can later return it.
            minimumLength = Utilities.GetMaxSizeForBucket(bucketIndex);
        }
        else if (minimumLength == 0)
        {
            // We allow requesting zero-length arrays (even though pooling such an array isn't valuable)
            // as it's a valid length array, and we want the pool to be usable in general instead of using
            // `new`, even for computed lengths. But, there's no need to log the empty array.  Our pool is
            // effectively infinite for empty arrays and we'll never allocate for rents and never store for returns.
            return [];
        }
        else
        {
            ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        }

        buffer = GC.AllocateUninitializedArray<T>(minimumLength, true);
        return buffer;
    }

    public override void Return(T[] array, bool clearArray = false)
    {
        // Determine with what bucket this array length is associated
        var bucketIndex = Utilities.SelectBucketIndex(array.Length);

        // Make sure our TLS buckets are initialized.  Technically we could avoid doing
        // this if the array being returned is erroneous or too large for the pool, but the
        // former condition is an error we don't need to optimize for, and the latter is incredibly
        // rare, given a max size of 1B elements.
        var tlsBuckets = t_tlsBuckets ?? InitializeTlsBucketsAndTrimming();

        if ((uint)bucketIndex >= (uint)tlsBuckets.Length) return;
        // Clear the array if the user requested it.
        if (clearArray) Array.Clear(array);

        // Check to see if the buffer is the correct size for this bucket.
        if (array.Length != Utilities.GetMaxSizeForBucket(bucketIndex))
            throw new ArgumentException("Buffer Not From Pool", nameof(array));

        // Store the array into the TLS bucket.  If there's already an array in it,
        // push that array down into the partitions, preferring to keep the latest
        // one in TLS for better locality.
        ref var tla = ref tlsBuckets[bucketIndex];
        var prev = tla.Array;
        tla = new SharedArrayPoolThreadLocalArray(array);
        if (prev is null) return; 
        var partitionsForArraySize = _buckets[bucketIndex] ?? CreatePerCorePartitions(bucketIndex);
        partitionsForArraySize.TryPush(prev);
    }

    private bool Trim()
    {
        var currentMilliseconds = Environment.TickCount;
        var pressure = Utilities.GetMemoryPressure();

        // Trim each of the per-core buckets.
        foreach (var p in _buckets) p?.Trim(currentMilliseconds, pressure);

        // Trim each of the TLS buckets. Note that threads may be modifying their TLS slots concurrently with
        // this trimming happening. We do not force synchronization with those operations, so we accept the fact
        // that we may end up firing a trimming event even if an array wasn't trimmed, and potentially
        // trim an array we didn't need to.  Both of these should be rare occurrences.

        // Under high pressure, release all thread locals.
        if (pressure == Utilities.MemoryPressure.High)
        {
            foreach (var tlsBuckets in _allTlsBuckets) Array.Clear(tlsBuckets.Key);
        }
        else
        {
            // Otherwise, release thread locals based on how long we've observed them to be stored. This time is
            // approximate, with the time set not when the array is stored but when we see it during a Trim, so it
            // takes at least two Trim calls (and thus two gen2 GCs) to drop an array, unless we're in high memory
            // pressure. These values have been set arbitrarily; we could tune them in the future.
            uint millisecondsThreshold = pressure switch
            {
                Utilities.MemoryPressure.Medium => 15_000,
                _ => 30_000
            };

            foreach (var tlsBuckets in _allTlsBuckets)
            {
                var buckets = tlsBuckets.Key;
                for (var i = 0; i < buckets.Length; i++)
                {
                    if (buckets[i].Array is null) continue;

                    // We treat 0 to mean it hasn't yet been seen in a Trim call. In the very rare case where Trim records 0,
                    // it'll take an extra Trim call to remove the array.
                    var lastSeen = buckets[i].MillisecondsTimeStamp;
                    if (lastSeen == 0)
                    {
                        buckets[i].MillisecondsTimeStamp = currentMilliseconds;
                    }
                    else if (currentMilliseconds - lastSeen >= millisecondsThreshold)
                    {
                        // Time noticeably wrapped, or we've surpassed the threshold.
                        // Clear out the array, and log its being trimmed if desired.
                        Interlocked.Exchange(ref buckets[i].Array, null);
                    }
                }
            }
        }

        return true;
    }

    private SharedArrayPoolThreadLocalArray[] InitializeTlsBucketsAndTrimming()
    {
        var tlsBuckets = new SharedArrayPoolThreadLocalArray[NumBuckets];
        t_tlsBuckets = tlsBuckets;
        _allTlsBuckets.Add(tlsBuckets, null);
        if (Interlocked.Exchange(ref _trimCallbackCreated, 1) == 0) 
            Gen2GcCallback.Register(s => ((PinnedArrayPool<T>)s).Trim(), this);
        return tlsBuckets;
    }
}

// The following partition types are separated out of SharedArrayPool<T> to avoid
// them being generic, in order to avoid the generic code size increase that would
// result, in particular for Native AOT. The only thing that's necessary to actually
// be generic is the return type of TryPop, and we can handle that at the access
// site with a well-placed Unsafe.As.

/// <summary>Wrapper for arrays stored in ThreadStatic buckets.</summary>
internal struct SharedArrayPoolThreadLocalArray(Array array)
{
    /// <summary>The stored array.</summary>
    public Array? Array = array;

    /// <summary>Environment.TickCount timestamp for when this array was observed by Trim.</summary>
    public int MillisecondsTimeStamp = 0;
}

/// <summary>Provides a collection of partitions, each of which is a pool of arrays.</summary>
internal sealed class SharedArrayPoolPartitions
{
    /// <summary>The partitions.</summary>
    private readonly Partition[] _partitions;

    /// <summary>Initializes the partitions.</summary>
    public SharedArrayPoolPartitions()
    {
        // Create the partitions.  We create as many as there are processors, limited by our max.
        var partitions = new Partition[SharedArrayPoolStatics.SPartitionCount];
        for (var i = 0; i < partitions.Length; i++) partitions[i] = new Partition();
        _partitions = partitions;
    }

    /// <summary>
    /// Try to push the array into any partition with available space, starting with partition associated with the current core.
    /// If all partitions are full, the array will be dropped.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPush(Array array)
    {
        // Try to push on to the associated partition first.  If that fails,
        // round-robin through the other partitions.
        var partitions = _partitions;
        var index = (int)((uint)Thread.GetCurrentProcessorId() %
                          (uint)SharedArrayPoolStatics.SPartitionCount); // mod by constant in tier 1
        for (var i = 0; i < partitions.Length; i++)
        {
            if (partitions[index].TryPush(array)) return true;
            if (++index == partitions.Length) index = 0;
        }

        return false;
    }

    /// <summary>
    /// Try to pop an array from any partition with available arrays, starting with partition associated with the current core.
    /// If all partitions are empty, null is returned.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Array? TryPop()
    {
        // Try to pop from the associated partition first.  If that fails, round-robin through the other partitions.
        var partitions = _partitions;
        var index = (int)((uint)Thread.GetCurrentProcessorId() %
                          (uint)SharedArrayPoolStatics.SPartitionCount); // mod by constant in tier 1
        for (var i = 0; i < partitions.Length; i++)
        {
            Array? arr;
            if ((arr = partitions[index].TryPop()) is not null) return arr;
            if (++index == partitions.Length) index = 0;
        }

        return null;
    }

    public void Trim(int currentMilliseconds, Utilities.MemoryPressure pressure)
    {
        foreach (var p in _partitions) p.Trim(currentMilliseconds, pressure);
    }

    /// <summary>Provides a simple, bounded stack of arrays, protected by a lock.</summary>
    private sealed class Partition
    {
        /// <summary>The arrays in the partition.</summary>
        private readonly Array?[] _arrays = new Array[SharedArrayPoolStatics.SMaxArraysPerPartition];

        /// <summary>Number of arrays stored in <see cref="_arrays"/>.</summary>
        private int _count;

        /// <summary>Timestamp set by Trim when it sees this as 0.</summary>
        private int _millisecondsTimestamp;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPush(Array array)
        {
            var enqueued = false;
            Monitor.Enter(this);
            var arrays = _arrays;
            var count = _count;
            if ((uint)count < (uint)arrays.Length)
            {
                if (count == 0)
                {
                    // Reset the time stamp now that we're transitioning from empty to non-empty.
                    // Trim will see this as 0 and initialize it to the current time when Trim is called.
                    _millisecondsTimestamp = 0;
                }

                Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(arrays), count) =
                    array; // arrays[count] = array, but avoiding stelemref
                _count = count + 1;
                enqueued = true;
            }

            Monitor.Exit(this);
            return enqueued;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Array? TryPop()
        {
            Array? arr = null;
            Monitor.Enter(this);
            var arrays = _arrays;
            var count = _count - 1;
            if ((uint)count < (uint)arrays.Length)
            {
                arr = arrays[count];
                arrays[count] = null;
                _count = count;
            }

            Monitor.Exit(this);
            return arr;
        }

        public void Trim(int currentMilliseconds, Utilities.MemoryPressure pressure)
        {
            const int TrimAfterMS = 60 * 1000; // Trim after 60 seconds for low/moderate pressure
            const int HighTrimAfterMS = 10 * 1000; // Trim after 10 seconds for high pressure

            if (_count == 0) return;

            var trimMilliseconds = pressure == Utilities.MemoryPressure.High ? HighTrimAfterMS : TrimAfterMS;

            lock (this)
            {
                if (_count == 0) return;

                if (_millisecondsTimestamp == 0)
                {
                    _millisecondsTimestamp = currentMilliseconds;
                    return;
                }

                if (currentMilliseconds - _millisecondsTimestamp <= trimMilliseconds) return;

                // We've elapsed enough time since the first item went into the partition.
                // Drop the top item(s) so they can be collected.

                var trimCount = pressure switch
                {
                    Utilities.MemoryPressure.High => SharedArrayPoolStatics.SMaxArraysPerPartition,
                    Utilities.MemoryPressure.Medium => 2,
                    _ => 1
                };

                while (_count > 0 && trimCount-- > 0) _arrays[--_count] = null;

                _millisecondsTimestamp = _count > 0
                    ? _millisecondsTimestamp + (trimMilliseconds / 4) // Give the remaining items a bit more time
                    : 0;
            }
        }
    }
}

internal static class SharedArrayPoolStatics
{
    /// <summary>Number of partitions to employ.</summary>
    internal static readonly int SPartitionCount = Environment.ProcessorCount;

    /// <summary>The maximum number of arrays per array size to store per partition.</summary>
    internal const int SMaxArraysPerPartition = 32;
}
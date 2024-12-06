using System.Buffers;

namespace Gravitas.PinnedPool;

public sealed class PinnedMemoryPool<T> : MemoryPool<T> where T : unmanaged
{
    public static readonly PinnedMemoryPool<T> Instance = new();

    public override int MaxBufferSize => Array.MaxLength;

    public override unsafe IMemoryOwner<T> Rent(int minimumBufferSize = -1)
    {
        if (minimumBufferSize == -1)
            minimumBufferSize = 1 + 4095 / sizeof(T);
        return new ArrayMemoryPoolBuffer(minimumBufferSize);
    }

    protected override void Dispose(bool disposing)
    {
    }

    private sealed class ArrayMemoryPoolBuffer(int size) : IMemoryOwner<T>
    {
        private T[]? _array = PinnedArrayPool<T>.Instance.Rent(size);

        public Memory<T> Memory
        {
            get
            {
                var array = _array;
                ObjectDisposedException.ThrowIf(array == null, this);
                return new Memory<T>(array);
            }
        }

        public void Dispose()
        {
            var array = _array;
            if (array == null) return;
            _array = null;
            PinnedArrayPool<T>.Instance.Return(array);
        }
    }
}
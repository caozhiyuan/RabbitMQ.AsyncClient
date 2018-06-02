using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Impl
{
    public class AsyncQueue<T>
    {
        private readonly SemaphoreSlim _sem;
        private readonly ConcurrentQueue<T> _que;

        public AsyncQueue()
        {
            _sem = new SemaphoreSlim(0);
            _que = new ConcurrentQueue<T>();
        }

        public void Enqueue(T item)
        {
            if (_cleared)
                throw new ObjectDisposedException("AsyncQueue Cleared");

            _que.Enqueue(item);
            _sem.Release();
        }

        public async Task<T> DequeueAsync(TimeSpan timmeout)
        {
            if (_cleared)
                throw new ObjectDisposedException("AsyncQueue Cleared");

            for (; ; )
            {
                var flag = await _sem.WaitAsync(timmeout);
                if (!flag)
                    return default(T);

                if (_que.TryDequeue(out var item))
                {
                    return item;
                }
            }
        }

        public int Count { get { return _que.Count; } }

        private volatile bool _cleared;

        public bool Cleared { get { return _cleared; } }

        public void Clear()
        {
            _cleared = true;

            for (; ; )
            {
                _sem.Dispose();

                if (!_que.TryDequeue(out var _))
                {
                    break;
                }
            }
        }
    }
}

#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.Queues;
using Testing.tables;

namespace Testing.basic_tests
{
    public class NamedQueue : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif

            var items = Enumerable.Range(0, 100).Select((f, i) => new QueuePoint()
            {
                Time = DateTime.Now.AddMinutes(i),
                Value = 1.01 + i
            })
            .ToList();

            db.NamedQueue<QueuePoint>("spy").PutReplaceInNamedQueue(items);

            var res = db.NamedQueue<QueuePoint>("spy").GetAllFromNamedQueue();

            if (res.Count() != items.Count() || res.First().Time != items.First().Time || res.First().Value != items.First().Value)
            {
                throw new Exception("Assert failure");
            }

            res = db.NamedQueue<QueuePoint>("msft").GetAllFromNamedQueue();

            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            db.NamedQueue<QueuePoint>("msft").PutReplaceInNamedQueue(items.Take(5).ToList());

            res = db.NamedQueue<QueuePoint>("msft").GetAllFromNamedQueue();

            if (res.Count() != 5)
            {
                throw new Exception("Assert failure");
            }

            db.NamedQueue<QueuePoint>("msft").PutReplaceInNamedQueue(new List<QueuePoint>());

            res = db.NamedQueue<QueuePoint>("spy").GetAllFromNamedQueue();

            if (res.Count() != items.Count() || res.First().Time != items.First().Time || res.First().Value != items.First().Value)
            {
                throw new Exception("Assert failure");
            }


            db.NamedQueue<QueuePoint>("spy").PutReplaceInNamedQueue(new List<QueuePoint>());
            res = db.NamedQueue<QueuePoint>("spy").GetAllFromNamedQueue();

            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
#endif
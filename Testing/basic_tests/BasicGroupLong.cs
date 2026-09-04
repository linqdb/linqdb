#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class BasicGroupLong : ITest
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
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<LongClass>().Delete(new HashSet<int>(db.Table<LongClass>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<LongClass>().RemoveGroupByMemoryIndex(f => f.IntValue, z => z.LongValueNonNull);
#endif

            var d = new LongClass()
            {
                Id = 1,
                IntValue = 5,
                LongValueNonNull = 55
            };
            db.Table<LongClass>().Save(d);

            d = new LongClass()
            {
                Id = 2,
                IntValue = 3,
                LongValueNonNull = 77
            };
            db.Table<LongClass>().Save(d);
            
            db.Table<LongClass>().CreateGroupByMemoryIndex(f => f.IntValue, z => z.LongValueNonNull);

            d = new LongClass()
            {
                Id = 3,
                LongValueNonNull = 23,
                IntValue = 10
            };
            db.Table<LongClass>().Save(d);
            d = new LongClass()
            {
                Id = 4,
                LongValueNonNull = 45,
                IntValue = 10
            };
            db.Table<LongClass>().Save(d);


            var res = db.Table<LongClass>()
                        .GroupBy(f => f.IntValue)
                        .Select(f => new
                        {
                            Key = f.Key,
                            Sum = f.Sum(z => z.LongValueNonNull),
                            Total = f.Count()
                        })
                        .ToList();

            if (res.Count() != 3 || res.Where(f => f.Key == 5).First().Total != 1 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 2 ||
                res.Where(f => f.Key == 5).First().Sum != 55 || res.Where(f => f.Key == 3).First().Sum != 77 || res.Where(f => f.Key == 10).First().Sum != 68)
            {
                throw new Exception("Assert failure");
            }

            db.Table<LongClass>().RemoveGroupByMemoryIndex(f => f.IntValue, z => z.LongValueNonNull);

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}

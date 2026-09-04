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
    class BasicGroupDecimal : ITest
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
            db.Table<DecimalClass>().Delete(new HashSet<int>(db.Table<DecimalClass>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<DecimalClass>().RemoveGroupByMemoryIndex(f => f.IntValue, z => z.DecValueNonNull);
#endif

            var d = new DecimalClass()
            {
                Id = 1,
                IntValue = 5,
                DecValueNonNull = 5.5m
            };
            db.Table<DecimalClass>().Save(d);

            d = new DecimalClass()
            {
                Id = 2,
                IntValue = 3,
                DecValueNonNull = 7.7m
            };
            db.Table<DecimalClass>().Save(d);
            
            db.Table<DecimalClass>().CreateGroupByMemoryIndex(f => f.IntValue, z => z.DecValueNonNull);

            d = new DecimalClass()
            {
                Id = 3,
                DecValueNonNull = 2.3m,
                IntValue = 10
            };
            db.Table<DecimalClass>().Save(d);
            d = new DecimalClass()
            {
                Id = 4,
                DecValueNonNull = 4.5m,
                IntValue = 10
            };
            db.Table<DecimalClass>().Save(d);


            var res = db.Table<DecimalClass>()
                        .GroupBy(f => f.IntValue)
                        .Select(f => new
                        {
                            Key = f.Key,
                            Sum = f.Sum(z => z.DecValueNonNull),
                            Total = f.Count()
                        })
                        .ToList();

            if (res.Count() != 3 || res.Where(f => f.Key == 5).First().Total != 1 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 2 ||
                res.Where(f => f.Key == 5).First().Sum != 5.5m || res.Where(f => f.Key == 3).First().Sum != 7.7m || res.Where(f => f.Key == 10).First().Sum != 6.8m)
            {
                throw new Exception("Assert failure");
            }

            db.Table<DecimalClass>().RemoveGroupByMemoryIndex(f => f.IntValue, z => z.DecValueNonNull);

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

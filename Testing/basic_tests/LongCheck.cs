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
    class LongCheck : ITest
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
#endif

            var d = new LongClass()
            {
                Id = 1,
                LongValue = 12,
                IntValue = 5
            };
            db.Table<LongClass>().Save(d);
            d = new LongClass()
            {
                Id = 2,
                LongValue = 23,
                IntValue = 10
            };
            db.Table<LongClass>().Save(d);
            d = new LongClass()
            {
                Id = 3,
                LongValue = 45,
                IntValue = 15
            };
            db.Table<LongClass>().Save(d);

            d = new LongClass()
            {
                Id = 1000,
                LongValue = null,
                IntValue = 5
            };

            db.Table<LongClass>().Save(d);
            var res = db.Table<LongClass>()
                        .Where(f => f.LongValue > 30)
                        .Select(f => new
                        {
                            f.LongValue
                        });
            if (res.Count() != 1 || res[0].LongValue != 45)
            {
                throw new Exception("Assert failure");
            }

            //negative
            d = new LongClass()
            {
                Id = 4,
                LongValue = -12,
                IntValue = 5
            };
            db.Table<LongClass>().Save(d);
            d = new LongClass()
            {
                Id = 5,
                LongValue = -23,
                IntValue = 10
            };
            db.Table<LongClass>().Save(d);
            d = new LongClass()
            {
                Id = 6,
                LongValue = -45,
                IntValue = 15
            };
            db.Table<LongClass>().Save(d);

            res = db.Table<LongClass>()
                        .Where(f => f.LongValue < -30)
                        .Select(f => new
                        {
                            f.LongValue
                        });
            if (res.Count() != 1 || res[0].LongValue != -45)
            {
                throw new Exception("Assert failure");
            }

            //misc
            res = db.Table<LongClass>()
                        .Where(f => f.LongValue < -30 || f.LongValue > 30)
                        .OrderByDescending(f => f.LongValue)
                        .Select(f => new
                        {
                            f.LongValue
                        });

            if (res.Count() != 2 || res[0].LongValue != 45 || res[1].LongValue != -45)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<LongClass>()
                        .Where(f => f.LongValue == 23 || f.LongValue == -23)
                        .OrderByDescending(f => f.LongValue)
                        .Select(f => new
                        {
                            f.LongValue
                        });

            if (res.Count() != 2 || res[0].LongValue != 23 || res[1].LongValue != -23)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<LongClass>()
                       .Intersect(f => f.LongValue, new HashSet<long?>(new List<long?>() { 23, -23 }))
                       .OrderByDescending(f => f.LongValue)
                       .Select(f => new
                       {
                           f.LongValue
                       });

            if (res.Count() != 2 || res[0].LongValue != 23 || res[1].LongValue != -23)
            {
                throw new Exception("Assert failure");
            }

            long? tmp = null;
            res = db.Table<LongClass>()
                       .Intersect(f => f.LongValue, new HashSet<long?>(new List<long?>() { tmp }))
                       .OrderByDescending(f => f.LongValue)
                       .Select(f => new
                       {
                           f.LongValue
                       });

            if (res.Count() != 1 || res[0].LongValue != null)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<LongClass>()
                       .Between(f => f.LongValue, -30, 30, BetweenBoundaries.BothInclusive)
                       .OrderByDescending(f => f.LongValue)
                       .Select(f => new
                       {
                           f.LongValue
                       });

            if (res.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<LongClass>()
                       .Between(f => f.LongValue, -30, 30, BetweenBoundaries.BothInclusive)
                       .OrderByDescending(f => f.LongValue)
                       .Select(f => new
                       {
                           f.LongValue
                       });

            if (res.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            long from = -30;
            long to = 30;
            res = db.Table<LongClass>()
                       .Between(f => f.LongValue, from, to, BetweenBoundaries.BothInclusive)
                       .OrderByDescending(f => f.LongValue)
                       .Select(f => new
                       {
                           f.LongValue
                       });

            if (res.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            var id = db.Table<LongClass>()
                      .Intersect(f => f.LongValue, new HashSet<long?>(new List<long?>() { tmp }))
                      .GetIds().Ids.Single();

            tmp = -777;
            db.Table<LongClass>().Update(f => f.LongValue, new Dictionary<int, long?>() { { id, tmp } });
            res = db.Table<LongClass>()
                      .Intersect(f => f.LongValue, new HashSet<long?>(new List<long?>() { tmp }))
                      .Select(f => new
                      {
                          f.LongValue
                      });

            if (res.Count() != 1 || res[0].LongValue != tmp)
            {
                throw new Exception("Assert failure");
            }


            db.Table<LongClass>().Delete(id);

            res = db.Table<LongClass>()
                      .Intersect(f => f.LongValue, new HashSet<long?>(new List<long?>() { tmp }))
                      .Select(f => new
                      {
                          f.LongValue
                      });

            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if (dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}

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
    class DecimalCheck : ITest
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
#endif

            var d = new DecimalClass()
            {
                Id = 1,
                DecValue = 1.2m,
                IntValue = 5
            };
            db.Table<DecimalClass>().Save(d);
            d = new DecimalClass()
            {
                Id = 2,
                DecValue = 2.3m,
                IntValue = 10
            };
            db.Table<DecimalClass>().Save(d);
            d = new DecimalClass()
            {
                Id = 3,
                DecValue = 4.5m,
                IntValue = 15
            };
            db.Table<DecimalClass>().Save(d);

            d = new DecimalClass()
            {
                Id = 1000,
                DecValue = null,
                IntValue = 5
            };

            db.Table<DecimalClass>().Save(d);
            var res = db.Table<DecimalClass>()
                        .Where(f => f.DecValue > 3m)
                        .Select(f => new
                        {
                            f.DecValue
                        });
            if (res.Count() != 1 || res[0].DecValue != 4.5m)
            {
                throw new Exception("Assert failure");
            }

            //negative
            d = new DecimalClass()
            {
                Id = 4,
                DecValue = -1.2m,
                IntValue = 5
            };
            db.Table<DecimalClass>().Save(d);
            d = new DecimalClass()
            {
                Id = 5,
                DecValue = -2.3m,
                IntValue = 10
            };
            db.Table<DecimalClass>().Save(d);
            d = new DecimalClass()
            {
                Id = 6,
                DecValue = -4.5m,
                IntValue = 15
            };
            db.Table<DecimalClass>().Save(d);

            res = db.Table<DecimalClass>()
                        .Where(f => f.DecValue < -3m)
                        .Select(f => new
                        {
                            f.DecValue
                        });
            if (res.Count() != 1 || res[0].DecValue != -4.5m)
            {
                throw new Exception("Assert failure");
            }

            //misc
            res = db.Table<DecimalClass>()
                        .Where(f => f.DecValue < -3m || f.DecValue > 3m)
                        .OrderByDescending(f => f.DecValue)
                        .Select(f => new
                        {
                            f.DecValue
                        });

            if (res.Count() != 2 || res[0].DecValue != 4.5m || res[1].DecValue != -4.5m)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<DecimalClass>()
                        .Where(f => f.DecValue == 2.3m || f.DecValue == -2.3m)
                        .OrderByDescending(f => f.DecValue)
                        .Select(f => new
                        {
                            f.DecValue
                        });

            if (res.Count() != 2 || res[0].DecValue != 2.3m || res[1].DecValue != -2.3m)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<DecimalClass>()
                       .Intersect(f => f.DecValue, new HashSet<decimal?>(new List<decimal?>() { 2.3m, -2.3m }))
                       .OrderByDescending(f => f.DecValue)
                       .Select(f => new
                       {
                           f.DecValue
                       });

            if (res.Count() != 2 || res[0].DecValue != 2.3m || res[1].DecValue != -2.3m)
            {
                throw new Exception("Assert failure");
            }

            decimal? tmp = null;
            res = db.Table<DecimalClass>()
                       .Intersect(f => f.DecValue, new HashSet<decimal?>(new List<decimal?>() { tmp }))
                       .OrderByDescending(f => f.DecValue)
                       .Select(f => new
                       {
                           f.DecValue
                       });

            if (res.Count() != 1 || res[0].DecValue != null)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<DecimalClass>()
                       .Between(f => f.DecValue, -3, 3, BetweenBoundaries.BothInclusive)
                       .OrderByDescending(f => f.DecValue)
                       .Select(f => new
                       {
                           f.DecValue
                       });

            if (res.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<DecimalClass>()
                       .Between(f => f.DecValue, -3.0m, 3.0m, BetweenBoundaries.BothInclusive)
                       .OrderByDescending(f => f.DecValue)
                       .Select(f => new
                       {
                           f.DecValue
                       });

            if (res.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            decimal from = -3.0m;
            decimal to = 3.0m;
            res = db.Table<DecimalClass>()
                       .Between(f => f.DecValue, from, to, BetweenBoundaries.BothInclusive)
                       .OrderByDescending(f => f.DecValue)
                       .Select(f => new
                       {
                           f.DecValue
                       });

            if (res.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            var id = db.Table<DecimalClass>()
                      .Intersect(f => f.DecValue, new HashSet<decimal?>(new List<decimal?>() { tmp }))
                      .GetIds().Ids.Single();

            tmp = -7.77m;
            db.Table<DecimalClass>().Update(f => f.DecValue, new Dictionary<int, decimal?>() { { id, tmp } });
            res = db.Table<DecimalClass>()
                      .Intersect(f => f.DecValue, new HashSet<decimal?>(new List<decimal?>() { tmp }))
                      .Select(f => new
                      {
                          f.DecValue
                      });

            if (res.Count() != 1 || res[0].DecValue != tmp)
            {
                throw new Exception("Assert failure");
            }


            db.Table<DecimalClass>().Delete(id);

            res = db.Table<DecimalClass>()
                      .Intersect(f => f.DecValue, new HashSet<decimal?>(new List<decimal?>() { tmp }))
                      .Select(f => new
                      {
                          f.DecValue
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

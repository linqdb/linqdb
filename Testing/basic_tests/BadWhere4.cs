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
    class BadWhere4 : ITest
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
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.Table<SomeData>().Save(d); 
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d); 
            d = new SomeData()
            {
                Id = 3,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.Table<SomeData>().Save(d);

            var res = GetData(15, db);
            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            var test = new BadWhere4TestClass();
            res = test.GetData(15, db);
            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            res = BadWhere4TestClass.GetDataStatic(15, db);
            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            
            var test2 = new BadWhere4TestNamespace.BadWhere4TestClass();
            res = test2.GetData(15, db);
            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            res = BadWhere4TestNamespace.BadWhere4TestClass.GetDataStatic(15, db);
            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            test.A = 15;
            res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == test.A)
                          .SelectEntity()
                          .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            test2.A = 15;
            res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == test2.A)
                          .SelectEntity()
                          .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            test.Composite = new BadWhere4TestClass() { A = 15, B = 15 };
            res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == test.Composite.A)
                          .SelectEntity()
                          .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            test.B = 15;
            res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == test.B)
                          .SelectEntity()
                          .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            test2.B = 15;
            res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == test2.B)
                          .SelectEntity()
                          .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == test.Composite.B)
                          .SelectEntity()
                          .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }


            Testing2.SomeData sd = new Testing2.SomeData()
            {
                PeriodId = 15
            };

            res = db.Table<SomeData>()
                         .Where(f => f.PeriodId == sd.PeriodId)
                         .SelectEntity()
                         .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            test.CompositeField = new BadWhere4TestClass() { A = 15, B = 15 };

            res = db.Table<SomeData>()
                         .Where(f => f.PeriodId == test.CompositeField.A)
                         .SelectEntity()
                         .ToList();


            test.Composite = new BadWhere4TestClass() { A = 16, B = 15 };

            res = db.Table<SomeData>()
                         .Where(f => f.PeriodId == test.Composite.A)
                         .SelectEntity()
                         .ToList();

            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            test.CompositeFieldAnotherNamesapce = new BadWhere4TestNamespace.BadWhere4TestClass() { A = 15, B = 15 };

            res = db.Table<SomeData>()
                         .Where(f => f.PeriodId == test.CompositeFieldAnotherNamesapce.A)
                         .SelectEntity()
                         .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 15)
            {
                throw new Exception("Assert failure");
            }

            BadWhere4TestClass.C = 10;
            res = db.Table<SomeData>()
                        .Where(f => f.PeriodId == BadWhere4TestClass.C)
                        .SelectEntity()
                        .ToList();

            if (res.Count() != 1 || res[0].PeriodId != 10)
            {
                throw new Exception("Assert failure");
            }

            test = new BadWhere4TestClass() { B = 10};
            res = test.GetData(test.B, db);
            if (res.Count() != 1 || res[0].PeriodId != 10)
            {
                throw new Exception("Assert failure");
            }

            test = new BadWhere4TestClass() { B = 10 };
            res = test.GetData(test, db);
            if (res.Count() != 1 || res[0].PeriodId != 10)
            {
                throw new Exception("Assert failure");
            }


#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public List<SomeData> GetData(int period_id, Db db)
        {
            var res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == period_id)
                          .SelectEntity()
                          .ToList();

            return res;
        }
        public string GetName()
        {
            return this.GetType().Name;
        }
    }
    class BadWhere4TestClass
    {
        public BadWhere4TestNamespace.BadWhere4TestClass CompositeFieldAnotherNamesapce;
        public BadWhere4TestClass CompositeField;
        public BadWhere4TestClass Composite { get; set; }
        public BadWhere4TestNamespace.BadWhere4TestClass CompositeAnotherNamesapce { get; set; }
        public int A { get; set; }
        public int B;
        public static int C;
        public List<SomeData> GetData(int period_id, Db db)
        {
            var res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == period_id)
                          .SelectEntity()
                          .ToList();

            return res;
        }

        public static List<SomeData> GetDataStatic(int period_id, Db db)
        {
            var res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == period_id)
                          .SelectEntity()
                          .ToList();

            return res;
        }

        public List<SomeData> GetData(BadWhere4TestClass t, Db db)
        {
            var res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == t.B)
                          .SelectEntity()
                          .ToList();

            return res;
        }
    }

}

namespace BadWhere4TestNamespace
{
    public class BadWhere4TestClass
    {
        public int A { get; set; }
        public int B;
        public List<SomeData> GetData(int period_id, Db db)
        {
            var res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == period_id)
                          .SelectEntity()
                          .ToList();

            return res;
        }

        public static List<SomeData> GetDataStatic(int period_id, Db db)
        {
            var res = db.Table<SomeData>()
                          .Where(f => f.PeriodId == period_id)
                          .SelectEntity()
                          .ToList();

            return res;
        }
    }
}

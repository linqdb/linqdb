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
    class PartitionedParallelSave : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public PartitionedParallelSave()
        {
        }

        public void Do(Db db)
        {
            Do2(db);
        }
        public void Do2(Db db)
        {
            bool dispose = false;
            string partition = "1";
            if (db == null)
            {
                dispose = true;
                db = new Db("DATA");
#if (SERVER)
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
                DoInit = true;
            }
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.PartitionedTable<UsersItem>(partition).Delete(new HashSet<int>(db.PartitionedTable<UsersItem>(partition).Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            this.db = db;

            PartitionedJobSave.errors = 0;
            PartitionedJobSave.n = 0;
            int total = 15000;
            //for (int j = 0; j < 5; j++)
            //{
                var jobs = new List<PartitionedJobSave>();
                for (int i = 0; i < total; i++)
                {
                    jobs.Add(new PartitionedJobSave());
                }

            //Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
            //{
            //    JobSave.Do(db);
            //});


            var list = new List<Task>();

            foreach (var job in jobs)
            {
                var t = Task.Run(() => PartitionedJobSave.Do(db));
                list.Add(t);
            }
            list.ForEach(f => f.Wait());
            
            if (PartitionedJobSave.errors > 0)
            {
                throw new Exception("Assert failure");
            }
            for (int i = 0; i < PartitionedJobSave.n; i++)
            {
                var r = db.PartitionedTable<UsersItem>(partition).Where(f => f.UserId == i).SelectEntity();
                if (r.Count() != 1 || r[0].UserId != i)
                {
                    //Console.WriteLine(r.Count() + " " + r[0].UserId + " " + i);
                    throw new Exception("Assert failure");
                }
            }
            if (db.PartitionedTable<UsersItem>(partition).Search(f => f.CodeSearch, "0").Count() != total)
            {
                //Console.WriteLine(db.Table<UsersItem>().Search(f => f.Code, "0").Count());
                throw new Exception("Assert failure");
            }

            if (DoInit)
            {
#if (SERVER || SOCKETS)
                if(dispose) { Logic.Dispose(); }
#else
                if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }

    class PartitionedJobSave
    {
        public static object _lock = new object();
        public static int errors = 0;
        public static int n = 0;
        static string partition = "1";
        public static int NextFixedNumber()
        {
            lock (_lock)
            {
                int tmp = n;
                n++;
                return tmp;
            }
        }

        public static void Do(Db db)
        {
            try
            {
                int n = NextFixedNumber();
                var d = new UsersItem()
                {
                    UserId = n
                };
                d.CodeSearch = "0 1 2";
                d.Date = DateTime.Now;
                d.GuidSearch = "asdasd";
                d.ID = "code_123";
                d.IsLive = 1;
                d.LangSearch = "1";
                d.TextSearch = "sdsdf ksajdks";
                d.TitleSearch = "0 1 2";

                db.PartitionedTable<UsersItem>(partition).Save(d);
            }
            catch (Exception ex)
            {
                Console.WriteLine("PartitionedParallel2: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}

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

namespace Testing.basic_tests
{
    

    class BoolType : ITest
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
            var d = new BoolClass();
            try
            {
                db.Table<BoolClass>().Save(d);
                throw new Exception("Assert failure");
            }
            catch(Exception e)
            {
                string msg = "type is not supported";
#if (SERVER)
                msg = "type is not supported";
#endif

                if (!e.Message.Contains(msg))
                {
                    throw new Exception("Assert failure");
                }
            }

#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if (Directory.Exists("DATA"))
            {
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif

        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }

    public class BoolClass
    {
        public int Id {get; set;}
        public bool B {get; set;}
    }
    public class DecimalClass
    {
        public int Id { get; set; }
        public int IntValue { get; set; }
        public decimal? DecValue { get; set; }
        public decimal DecValueNonNull { get; set; }
    }
    public class LongClass
    {
        public int Id { get; set; }
        public int IntValue { get; set; }
        public long? LongValue { get; set; }
        public long LongValueNonNull { get; set; }
    }
}

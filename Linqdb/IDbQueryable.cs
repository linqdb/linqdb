using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public class IDbQueryable<T>
    {
        public Ldb _db { get; set; }
        public QueryTree LDBTree = new QueryTree();
        public LinqdbTransactionInternal LDBTransaction { get; set; }
        string _partition { get; set; }
        public string Partition
        {
            get
            {
                return _partition;
            }
            set
            { 
                _partition = value;
                LDBTree.Partition = value;
            }
        }
    }

    public class IDbOrderedQueryable<T>
    {
        public Ldb _db { get; set; }
        public QueryTree LDBTree = new QueryTree();
        string _partition { get; set; }
        public string Partition
        {
            get
            {
                return _partition;
            }
            set
            {
                _partition = value;
                LDBTree.Partition = value;
            }
        }
    }

    public class IDbGroupedQueryable<T, TKey>
    {
        public Ldb _db { get; set; }
        public QueryTree LDBTree = new QueryTree();
    }
}

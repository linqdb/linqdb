using LinqDb;
using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public int? AtomicIncrement<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, int value, T item, int? old_val_equals) where T : new()
        {
            if (item == null)
            {
                throw new LinqDbException("Linqdb: new_item_if_doesnt_exist can't be null");
            }
            var table_info = CheckTableInfo<T>(source.Partition);
            var type_name = GetTypeName<T>(source.Partition);
            var _write_lock = GetTableWriteLock(type_name);
            lock (_write_lock)
            {
                var statistics = new LinqdbSelectStatisticsInternal();
                var res = this.SelectEntity<T>(source.LDBTree, statistics);
                if (res != null && res.Count() > 1)
                {
                    throw new LinqDbException("Linqdb: more than one item identified");
                }
                if (res == null || !res.Any())
                {
                    this.SaveIncrement<T>(item, null);
                    return null;
                }
                else
                {
                    var par = keySelector.Parameters.First();
                    var name = SharedUtils.GetPropertyName(keySelector);
                    if (name == "Id")
                    {
                        throw new LinqDbException("Linqdb: can't modify Id property");
                    }
                    object ov = typeof(T).GetProperty(name).GetValue(res[0], null);
                    if (ov == null || !(ov is int))
                    {
                        throw new LinqDbException("Linqdb: property to increment must be of type int");
                    }
                    int old_val = (int)ov;
                    if (old_val_equals == null || old_val_equals == old_val)
                    {
                        int id = (int)typeof(T).GetProperty("Id").GetValue(res[0], null);
                        var dic = new Dictionary<int, int?>() { { id, old_val + value } };
                        UpdateIncrement(keySelector, dic, null);
                    }
                    return old_val;
                }
            }
        }

        ulong GetWhereHash(string table_name, QueryTree tree)
        {
            if (tree.WhereInfo == null || tree.WhereInfo.Count() != 1)
            {
                throw new LinqDbException("Linqdb: one (and only) .Where statement should precede AtomicIncrement");
            }

            StringBuilder res = new StringBuilder();
            res.Append(table_name + "|");
            var whereInfo = tree.WhereInfo[0];
            foreach (var op in whereInfo.Opers)
            {
                res.Append(op.IsOperator + "|" + op.Type + "|" + op.ColumnName + "|" + op.NonDbValue + "|" + op.IsDb + "|" + op.TableNumber + "|" + op.ColumnNumber + "|" + op.ColumnType + "|" + op.IsResult + "|");
            }

            return CalculateHash(res.ToString());
        }
       
        public void AtomicIncremen2Props<T, TKey1, TKey2>(IDbQueryable<T> source, Expression<Func<T, TKey1>> keySelector1, Expression<Func<T, TKey2>> keySelector2, int value1, int value2, T item) where T : new()
        {
            if (item == null)
            {
                throw new LinqDbException("Linqdb: new_item_if_doesnt_exist can't be null");
            }
            var table_info = CheckTableInfo<T>(source.Partition);
            var type_name = GetTypeName<T>(source.Partition);
            var _write_lock = GetTableWriteLock(type_name);
            lock (_write_lock)
            {
                var statistics = new LinqdbSelectStatisticsInternal();
                var res = this.SelectEntity<T>(source.LDBTree, statistics);
                if (res != null && res.Count() > 1)
                {
                    throw new LinqDbException("Linqdb: more than one item identified");
                }
                if (res == null || !res.Any())
                {
                    this.SaveIncrement<T>(item, null);
                }
                else
                {
                    //prop1
                    var par1 = keySelector1.Parameters.First();
                    var name1 = SharedUtils.GetPropertyName(keySelector1); 
                    if (name1 == "Id")
                    {
                        throw new LinqDbException("Linqdb: can't modify Id property");
                    }
                    object ov = typeof(T).GetProperty(name1).GetValue(res[0], null);
                    if (ov == null || !(ov is int))
                    {
                        throw new LinqDbException("Linqdb: property to increment must be of type int");
                    }
                    int old_val = (int)ov;
                    int id = (int)typeof(T).GetProperty("Id").GetValue(res[0], null);
                    var dic = new Dictionary<int, int?>() { { id, old_val + value1 } };
                    UpdateIncrement(keySelector1, dic, null);

                    //prop2
                    var par2 = keySelector2.Parameters.First();
                    var name2 = SharedUtils.GetPropertyName(keySelector2);
                    if (name2 == "Id")
                    {
                        throw new LinqDbException("Linqdb: can't modify Id property");
                    }
                    ov = typeof(T).GetProperty(name2).GetValue(res[0], null);
                    if (ov == null || !(ov is int))
                    {
                        throw new LinqDbException("Linqdb: property to increment must be of type int");
                    }
                    old_val = (int)ov;
                    dic = new Dictionary<int, int?>() { { id, old_val + value2 } };
                    UpdateIncrement(keySelector2, dic, null);
                }
            }
        }
    }
}

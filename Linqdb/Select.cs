using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using static ServerSharedData.SharedUtils;

namespace LinqDbInternal
{
    public class EncodedValue
    {
        public LinqDbTypes Type { get; set; }
        public bool IsNull { get; set; }
        public double DoubleVal { get; set; }
        public decimal DecimalVal { get; set; }
        public int IntVal { get; set; }
        public long LongVal { get; set; }
        public byte[] BinValue { get; set; }
        public byte[] StringValue { get; set; }
        public string FullString { get; set; }
    }
    public partial class Ldb
    {
        public EncodedValue EncodeValue(LinqDbTypes column_type, object val)
        {
            if (val == null)
            {
                return new EncodedValue()
                {
                    Type = column_type,
                    IsNull = true
                };
            }

            if (column_type == LinqDbTypes.DateTime_)
            {
                return new EncodedValue()
                {
                    Type = column_type,
                    DoubleVal = val is DateTime ? (Convert.ToDateTime(val) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds : (double)val
                };
            }

            if (column_type == LinqDbTypes.double_)
            {
                return new EncodedValue()
                {
                    Type = column_type,
                    DoubleVal = Convert.ToDouble(val)
                };
            }

            if (column_type == LinqDbTypes.decimal_)
            {
                return new EncodedValue()
                {
                    Type = column_type,
                    DecimalVal = Convert.ToDecimal(val)
                };
            }

            if (column_type == LinqDbTypes.int_)
            {
                return new EncodedValue()
                {
                    Type = column_type,
                    IntVal = Convert.ToInt32(val)
                };
            }

            if (column_type == LinqDbTypes.long_)
            {
                return new EncodedValue()
                {
                    Type = column_type,
                    LongVal = Convert.ToInt64(val)
                };
            }

            if (column_type == LinqDbTypes.binary_ || column_type == LinqDbTypes.vector_)
            {
                if (((byte[])val).Length == 0)
                {
                    return new EncodedValue()
                    {
                        Type = column_type,
                        IsNull = true
                    };
                }
                return new EncodedValue()
                {
                    Type = column_type,
                    BinValue = (byte[])val
                };
            }

            if (column_type == LinqDbTypes.string_)
            {
                if (((string)val) == "")
                {
                    return new EncodedValue()
                    {
                        Type = column_type,
                        IsNull = true
                    };
                }
                return new EncodedValue()
                {
                    Type = column_type,
                    StringValue = CalculateMD5Hash((SharedUtils.NormaliseString((string)val))),
                    FullString = (string)val
                };
            }

            return null;
        }
        public class ReadByteCount
        {
            public int read_size_bytes { get; set; }
        }
        public List<T> SelectEntity<T>(QueryTree tree, LinqdbSelectStatisticsInternal statistics) where T : new()
        {
            CheckTableInfo<T>(tree.Partition);

            using (var snapshot = leveld_db.CreateSnapshot())
            {
                var ro = new ReadOptions().SetSnapshot(snapshot);
                DateTime start = DateTime.Now;
                var type_name = GetTypeName<T>(tree.Partition);
                var table_info = GetTableInfo(type_name);
                var where_res = CalculateWhereResult<T>(tree, table_info, ro);
                where_res = FindBetween(tree, where_res, ro);
                where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                double? searchPercentile = null;
                where_res = Search(tree, where_res, ro, out searchPercentile);
                int row_count = GetTableRowCount(table_info, ro);

                var fres = CombineData(where_res);
                statistics.Total = fres.All ? row_count : fres.ResIds.Count();
                statistics.SearchedPercentile = searchPercentile;
                var distances = where_res.FirstOrDefault(f => f.ResDistances != null && f.ResDistances.Any());
                if (distances != null)
                {
                    statistics.Distances = new Dictionary<int, double>();
                    for (int i = 0; i < distances.ResDistances.Count(); i++)
                    {
                        statistics.Distances[distances.ResIds[i]] = distances.ResDistances[i];
                    }
                }
                fres = OrderData(fres, tree, row_count, ro, table_info);
                int count = fres.All ? row_count : fres.ResIds.Count();
                Dictionary<string, Tuple<List<int>, List<byte>>> data = new Dictionary<string, Tuple<List<int>, List<byte>>>();

                var read_counter = new ReadByteCount();
                var props = typeof(T).GetProperties();
                foreach (PropertyInfo prop in props)
                {
                    var name = prop.Name;
                    if (fres.All)
                    {
                        data[name] = ReadAllValues(name, table_info, fres, ro, read_counter, count);
                    }
                    else if ((fres.ResIds.Count() / (double)row_count) > 0.40 && fres.ResIds.Count() > 500000)
                    {
                        data[name] = ReadSomeValuesIterator(name, table_info, fres, ro, read_counter, count, row_count);
                    }
                    else
                    {
                        data[name] = ReadSomeValues(name, table_info, fres, tree.QueryCache, ro, read_counter, count, row_count);
                    }
                }

                //if (fres.IsOrdered && !data.Keys.Contains("Id"))
                //{
                //    if ((fres.ResIds.Count() / (double)row_count) > 0.40 && fres.ResIds.Count() > 500000)
                //    {
                //        data["Id"] = ReadSomeValuesIterator("Id", table_info, fres, ro, read_counter, count);
                //    }
                //    else
                //    {
                //        data["Id"] = ReadSomeValues("Id", table_info, fres, tree.QueryCache, ro, read_counter, count);
                //    }
                //}


                List<T> result = new List<T>();
                List<KeyValuePair<int, T>> ordered_result = new List<KeyValuePair<int, T>>();

                //current_ids fuss is about the fact that any column may have data gaps in multiple places (column added, removed, added ...)
                //the only column that doesn't have gaps is Id
                //assumption is that ids in data[name].Item1 is in ascending order
                int[] currents = new int[props.Count()];
                int[] currents_ids = new int[props.Count()];
                Tuple<List<int>, List<byte>>[] data_array = new Tuple<List<int>, List<byte>>[props.Count()];
                int id_nr = 0;
                for (int i = 0; i < props.Count(); i++)
                {
                    if (props[i].Name == "Id")
                    {
                        id_nr = i;
                    }
                    currents[i] = 0;
                    currents_ids[i] = 0;
                    data_array[i] = data[props[i].Name];
                }
                for (int i = 0; i < count; i++)
                {
                    int id = data_array[id_nr].Item1[currents_ids[id_nr]];

                    List<object> cdata = new List<object>();
                    int cc = -1;
                    foreach (PropertyInfo prop in props)
                    {
                        cc++;
                        var name = prop.Name;
                        byte[] val = null;

                        var by = data_array[cc];
                        bool skip = false;
                        if (currents_ids[cc] >= by.Item1.Count || id < by.Item1[currents_ids[cc]])
                        {
                            skip = true;
                        }
                        else
                        {
                            currents_ids[cc]++;
                        }
                        if (by.Item2.Any() && !skip)
                        {
                            if (table_info.Columns[name] == LinqDbTypes.int_)
                            {
                                if (by.Item2[currents[cc]] == 1)
                                {
                                    currents[cc]++;
                                    val = new byte[4];
                                    for (int j = 0, k = currents[cc]; j < 4; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[cc] += 4;
                                }
                                else if (by.Item2[currents[cc]] == 250)
                                {
                                    currents[cc]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[cc]++;
                                }
                            }
                            else if (table_info.Columns[name] == LinqDbTypes.long_)
                            {
                                if (by.Item2[currents[cc]] == 1)
                                {
                                    currents[cc]++;
                                    val = new byte[8];
                                    for (int j = 0, k = currents[cc]; j < 8; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[cc] += 8;
                                }
                                else if (by.Item2[currents[cc]] == 250)
                                {
                                    currents[cc]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[cc]++;
                                }
                            }
                            else if (table_info.Columns[name] == LinqDbTypes.double_ || table_info.Columns[name] == LinqDbTypes.DateTime_)
                            {
                                if (by.Item2[currents[cc]] == 1)
                                {
                                    currents[cc]++;
                                    val = new byte[8];
                                    for (int j = 0, k = currents[cc]; j < 8; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[cc] += 8;
                                }
                                else if (by.Item2[currents[cc]] == 250)
                                {
                                    currents[cc]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[cc]++;
                                }
                            }
                            else if (table_info.Columns[name] == LinqDbTypes.decimal_)
                            {
                                if (by.Item2[currents[cc]] == 1)
                                {
                                    currents[cc]++;
                                    val = new byte[32];
                                    for (int j = 0, k = currents[cc]; j < 32; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[cc] += 32;
                                }
                                else if (by.Item2[currents[cc]] == 250)
                                {
                                    currents[cc]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[cc]++;
                                }
                            }
                            else
                            {
                                int size = BitConverter.ToInt32(new byte[4] { by.Item2[currents[cc]], by.Item2[currents[cc] + 1], by.Item2[currents[cc] + 2], by.Item2[currents[cc] + 3] }, 0);
                                currents[cc] += 4;
                                if (size > 0)
                                {
                                    val = new byte[size];
                                    for (int j = 0, k = currents[cc]; j < size; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[cc] += size;
                                }
                                else if (size == -2)
                                {
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                            }
                        }
                        if (table_info.Columns[name] != LinqDbTypes.binary_ && table_info.Columns[name] != LinqDbTypes.vector_ && table_info.Columns[name] != LinqDbTypes.string_)
                        {
                            if (val == null)
                            {
                                Type type = prop.PropertyType;
                                var is_nullable = Nullable.GetUnderlyingType(type) != null;

                                if (!is_nullable)
                                {
                                    switch (table_info.Columns[name])
                                    {
                                        case LinqDbTypes.DateTime_:
                                            cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
                                            break;
                                        case LinqDbTypes.double_:
                                            cdata.Add((double)0);
                                            break;
                                        case LinqDbTypes.decimal_:
                                            cdata.Add((decimal)0);
                                            break;
                                        case LinqDbTypes.int_:
                                            cdata.Add(0);
                                            break;
                                        case LinqDbTypes.long_:
                                            cdata.Add((long)0);
                                            break;
                                        default:
                                            throw new LinqDbException("Linqdb: Unsupported type");
                                    }
                                }
                                else
                                {
                                    switch (table_info.Columns[name])
                                    {
                                        case LinqDbTypes.DateTime_:
                                            cdata.Add(null);
                                            break;
                                        case LinqDbTypes.double_:
                                            cdata.Add(null);
                                            break;
                                        case LinqDbTypes.decimal_:
                                            cdata.Add(null);
                                            break;
                                        case LinqDbTypes.int_:
                                            cdata.Add(null);
                                            break;
                                        case LinqDbTypes.long_:
                                            cdata.Add(null);
                                            break;
                                        default:
                                            throw new LinqDbException("Linqdb: Unsupported type");
                                    }
                                }
                            }
                            else if (ValsEqual(val, NullConstant))
                            {
                                cdata.Add(null);
                            }
                            else
                            {
                                switch (table_info.Columns[name])
                                {
                                    case LinqDbTypes.DateTime_:
                                        cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(BitConverter.ToDouble(val.MyReverseWithCopy(LinqDbTypes.DateTime_), 0)));
                                        break;
                                    case LinqDbTypes.double_:
                                        cdata.Add(BitConverter.ToDouble(val.MyReverseWithCopy(LinqDbTypes.double_), 0));
                                        break;
                                    case LinqDbTypes.decimal_:
                                        cdata.Add(DecimalConversion.FromByteArray(val));
                                        break;
                                    case LinqDbTypes.int_:
                                        var int_v = BitConverter.ToInt32(val.MyReverseWithCopy(LinqDbTypes.int_), 0);
                                        //if (name == "Id")
                                        //{
                                        //    id = int_v;
                                        //}
                                        cdata.Add(int_v);
                                        break;
                                    case LinqDbTypes.long_:
                                        var long_v = BitConverter.ToInt64(val.MyReverseWithCopy(LinqDbTypes.long_), 0);
                                        cdata.Add(long_v);
                                        break;
                                    default:
                                        throw new LinqDbException("Linqdb: Unsupported type");
                                }
                            }
                        }
                        else if (table_info.Columns[name] == LinqDbTypes.binary_ || table_info.Columns[name] == LinqDbTypes.vector_)
                        {
                            if (val == null || val.Length == 1)
                            {
                                cdata.Add(null);
                            }
                            else
                            {
                                cdata.Add(val.Skip(1).ToArray());
                            }
                        }
                        else
                        {
                            if (val == null || val.Length == 1)
                            {
                                cdata.Add(null);
                            }
                            else
                            {
                                cdata.Add(Encoding.Unicode.GetString(val.Skip(1).ToArray()));
                            }
                        }
                    }
                    if (fres.IsOrdered)
                    {
                        var cresult = new T();
                        int j = 0;
                        //foreach (var propInfo in props)
                        //{
                        //    propInfo.SetValue(cresult, cdata[j]);
                        //    j++;
                        //}

                        foreach (var propInfo in props)
                        {
                            var value = cdata[j];
                            if (table_info.Columns[propInfo.Name] == LinqDbTypes.vector_)
                            {
                                // Convert byte[] → float[]
                                var floats = FloatByteConverter.ByteArrayToFloatArray(value as byte[]);
                                propInfo.SetValue(cresult, floats);
                            }
                            else
                            {
                                // Set directly for all other types
                                propInfo.SetValue(cresult, value);
                            }
                            j++;
                        }

                        ordered_result.Add(new KeyValuePair<int, T>(id, cresult));
                    }
                    else
                    {
                        var cresult = new T();
                        int j = 0;
                        //foreach (var propInfo in props)
                        //{
                        //    propInfo.SetValue(cresult, cdata[j]);
                        //    j++;
                        //}
                        foreach (var propInfo in props)
                        {
                            var value = cdata[j];
                            if (table_info.Columns[propInfo.Name] == LinqDbTypes.vector_)
                            {
                                // Convert byte[] → float[]
                                var floats = FloatByteConverter.ByteArrayToFloatArray(value as byte[]);
                                propInfo.SetValue(cresult, floats);
                            }
                            else
                            {
                                // Set directly for all other types
                                propInfo.SetValue(cresult, value);
                            }
                            j++;
                        }
                        result.Add(cresult);
                    }
                }

                if (fres.IsOrdered)
                {
                    result = ordered_result.OrderBy(f => fres.OrderedIds[f.Key]).Select(f => f.Value).ToList();
                }
                return result;
            }
        }

        public List<R> Select<T, R>(QueryTree tree, Expression<Func<T, R>> predicate, LinqdbSelectStatisticsInternal statistics) where T : new()
        {
            CheckTableInfo<T>(tree.Partition);
            using (var snapshot = leveld_db.CreateSnapshot())
            {
                CompiledInfo<R> cinfo = new CompiledInfo<R>();
                var ro = new ReadOptions().SetSnapshot(snapshot);
                DateTime start = DateTime.Now;
                var type_name = GetTypeName<T>(tree.Partition);
                var table_info = GetTableInfo(type_name);
                var where_res = CalculateWhereResult<T>(tree, table_info, ro);
                where_res = FindBetween(tree, where_res, ro);
                where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                double? searchPercentile = null;
                where_res = Search(tree, where_res, ro, out searchPercentile);
                statistics.SearchedPercentile = searchPercentile;
                var distances = where_res.FirstOrDefault(f => f.ResDistances != null && f.ResDistances.Any());
                if (distances != null)
                {
                    statistics.Distances = new Dictionary<int, double>();
                    for (int i = 0; i < distances.ResDistances.Count(); i++)
                    {
                        statistics.Distances[distances.ResIds[i]] = distances.ResDistances[i];
                    }
                }
                int row_count = GetTableRowCount(table_info, ro);

                var fres = CombineData(where_res);
                statistics.Total = fres.All ? row_count : fres.ResIds.Count();
                fres = OrderData(fres, tree, row_count, ro, table_info);
                int count = fres.All ? row_count : fres.ResIds.Count();
                var body = predicate.Body;
                var new_expr = body as NewExpression;
                if (new_expr == null)
                {
                    throw new LinqDbException("Linqdb: Select must be with anonymous type, for example, .Select(f => new { Id = f.Id })");
                }


                Dictionary<string, Tuple<List<int>, List<byte>>> data = new Dictionary<string, Tuple<List<int>, List<byte>>>();
                var read_counter = new ReadByteCount();
                foreach (var a in new_expr.Arguments)
                {
                    var name = a.ToString().Split(".".ToCharArray())[1];
                    if (!table_info.Columns.ContainsKey(name))
                    {
                        throw new LinqDbException("Linqdb: Select must not have any expressions, i.e. .Select(f => new { Id = f.Id + 1}) won't work.");
                    }
                    if (fres.All)
                    {
                        data[name] = ReadAllValues(name, table_info, fres, ro, read_counter, count);
                    }
                    else if ((fres.ResIds.Count() / (double)row_count) > 0.40 && fres.ResIds.Count() > 500000)
                    {
                        data[name] = ReadSomeValuesIterator(name, table_info, fres, ro, read_counter, count, row_count);
                    }
                    else
                    {
                        data[name] = ReadSomeValues(name, table_info, fres, tree.QueryCache, ro, read_counter, count, row_count);
                    }
                }

                if (/*fres.IsOrdered && */!data.Keys.Contains("Id"))
                {
                    if (fres.All)
                    {
                        data["Id"] = ReadAllValues("Id", table_info, fres, ro, read_counter, count);
                    }
                    else if ((fres.ResIds.Count() / (double)row_count) > 0.40 && fres.ResIds.Count() > 500000)
                    {
                        data["Id"] = ReadSomeValuesIterator("Id", table_info, fres, ro, read_counter, count, row_count);
                    }
                    else
                    {
                        data["Id"] = ReadSomeValues("Id", table_info, fres, tree.QueryCache, ro, read_counter, count, row_count);
                    }
                }

                List<string> names = new List<string>();
                foreach (var a in new_expr.Arguments)
                {
                    names.Add(a.ToString().Split(new char[1] { '.' })[1]);
                }

                List<R> result = new List<R>();
                List<KeyValuePair<int, R>> ordered_result = new List<KeyValuePair<int, R>>();

                var props = typeof(T).GetProperties();
                int[] currents = new int[props.Count() + 1];
                int[] currents_ids = new int[props.Count() + 1];
                int id_nr = 0;
                Tuple<List<int>, List<byte>>[] data_array = new Tuple<List<int>, List<byte>>[props.Count() + 1];
                for (int i = 0; i < props.Count(); i++)
                {
                    if (props[i].Name == "Id")
                    {
                        id_nr = i;
                    }
                    currents[i] = 0;
                    currents_ids[i] = 0;
                }

                var cc = new Dictionary<string, int>();
                int cc_ = -1;
                foreach (var name in names.Where(f => f != "Id"))
                {
                    cc_++;
                    if (cc_ == id_nr)
                    {
                        cc_++;
                    }
                    cc[name] = cc_;
                    data_array[cc_] = data[name];
                }
                cc["Id"] = id_nr;
                data_array[id_nr] = data["Id"];

                List<int> pos = null;
                for (int i = 0; i < count; i++)
                {
                    int id = data_array[id_nr].Item1[currents_ids[id_nr]];
                    if (!names.Contains("Id"))
                    {
                        currents_ids[id_nr]++;
                    }
                    List<object> cdata = new List<object>();

                    foreach (var name in names)
                    {
                        int nr = cc[name];
                        var by = data_array[nr];
                        byte[] val = null;

                        bool skip = false;
                        if (currents_ids[nr] >= by.Item1.Count || id < by.Item1[currents_ids[nr]])
                        {
                            skip = true;
                        }
                        else
                        {
                            currents_ids[nr]++;
                        }
                        if (by.Item2.Any() && !skip)
                        {
                            if (table_info.Columns[name] == LinqDbTypes.int_)
                            {
                                if (by.Item2[currents[nr]] == 1)
                                {
                                    currents[nr]++;
                                    val = new byte[4];
                                    for (int j = 0, k = currents[nr]; j < 4; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[nr] += 4;
                                }
                                else if (by.Item2[currents[nr]] == 250)
                                {
                                    currents[nr]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[nr]++;
                                }
                            }
                            else if (table_info.Columns[name] == LinqDbTypes.long_)
                            {
                                if (by.Item2[currents[nr]] == 1)
                                {
                                    currents[nr]++;
                                    val = new byte[8];
                                    for (int j = 0, k = currents[nr]; j < 8; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[nr] += 8;
                                }
                                else if (by.Item2[currents[nr]] == 250)
                                {
                                    currents[nr]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[nr]++;
                                }
                            }
                            else if (table_info.Columns[name] == LinqDbTypes.double_ || table_info.Columns[name] == LinqDbTypes.DateTime_)
                            {
                                if (by.Item2[currents[nr]] == 1)
                                {
                                    currents[nr]++;
                                    val = new byte[8];
                                    for (int j = 0, k = currents[nr]; j < 8; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[nr] += 8;
                                }
                                else if (by.Item2[currents[nr]] == 250)
                                {
                                    currents[nr]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[nr]++;
                                }
                            }
                            else if (table_info.Columns[name] == LinqDbTypes.decimal_)
                            {
                                if (by.Item2[currents[nr]] == 1)
                                {
                                    currents[nr]++;
                                    val = new byte[32];
                                    for (int j = 0, k = currents[nr]; j < 32; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[nr] += 32;
                                }
                                else if (by.Item2[currents[nr]] == 250)
                                {
                                    currents[nr]++;
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                                else
                                {
                                    currents[nr]++;
                                }
                            }
                            else
                            {
                                int size = BitConverter.ToInt32(new byte[4] { by.Item2[currents[nr]], by.Item2[currents[nr] + 1], by.Item2[currents[nr] + 2], by.Item2[currents[nr] + 3] }, 0);
                                currents[nr] += 4;
                                if (size > 0)
                                {
                                    val = new byte[size];
                                    for (int j = 0, k = currents[nr]; j < size; j++, k++)
                                    {
                                        val[j] = by.Item2[k];
                                    }
                                    currents[nr] += size;
                                }
                                else if (size == -2)
                                {
                                    val = new byte[1];
                                    val[0] = NullConstant[0];
                                }
                            }
                        }

                        if (table_info.Columns[name] != LinqDbTypes.binary_ && table_info.Columns[name] != LinqDbTypes.vector_ && table_info.Columns[name] != LinqDbTypes.string_)
                        {
                            if (val == null)
                            {
                                switch (table_info.Columns[name])
                                {
                                    case LinqDbTypes.DateTime_:
                                        cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
                                        break;
                                    case LinqDbTypes.double_:
                                        cdata.Add((double)0);
                                        break;
                                    case LinqDbTypes.decimal_:
                                        cdata.Add((decimal)0);
                                        break;
                                    case LinqDbTypes.int_:
                                        cdata.Add(0);
                                        break;
                                    case LinqDbTypes.long_:
                                        cdata.Add((long)0);
                                        break;
                                    default:
                                        throw new LinqDbException("Linqdb: Unsupported type");
                                }
                            }
                            else if (ValsEqual(val, NullConstant))
                            {
                                cdata.Add(null);
                            }
                            else
                            {
                                switch (table_info.Columns[name])
                                {
                                    case LinqDbTypes.DateTime_:
                                        cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(BitConverter.ToDouble(val.MyReverseWithCopy(LinqDbTypes.DateTime_), 0)));
                                        break;
                                    case LinqDbTypes.double_:
                                        cdata.Add(BitConverter.ToDouble(val.MyReverseWithCopy(LinqDbTypes.double_), 0));
                                        break;
                                    case LinqDbTypes.decimal_:
                                        cdata.Add(DecimalConversion.FromByteArray(val));
                                        break;
                                    case LinqDbTypes.int_:
                                        var int_v = BitConverter.ToInt32(val.MyReverseWithCopy(LinqDbTypes.int_), 0);
                                        if (name == "Id")
                                        {
                                            id = int_v;
                                        }
                                        cdata.Add(int_v);
                                        break;
                                    case LinqDbTypes.long_:
                                        var long_v = BitConverter.ToInt64(val.MyReverseWithCopy(LinqDbTypes.long_), 0);
                                        cdata.Add(long_v);
                                        break;
                                    default:
                                        throw new LinqDbException("Linqdb: Unsupported type");
                                }
                            }
                        }
                        else if (table_info.Columns[name] == LinqDbTypes.binary_ || table_info.Columns[name] == LinqDbTypes.vector_)
                        {
                            if (val == null || val.Length == 1)
                            {
                                cdata.Add(null);
                            }
                            else
                            {
                                cdata.Add(val.Skip(1).ToArray());
                            }
                        }
                        else
                        {
                            if (val == null || val.Length == 1)
                            {
                                cdata.Add(null);
                            }
                            else
                            {
                                cdata.Add(Encoding.Unicode.GetString(val.Skip(1).ToArray()));
                            }
                        }
                    }

                    pos = CoerceCtorFloatArgs<R>(cdata, pos);

                    if (fres.IsOrdered)
                    {
                        //var cresult = (R)Activator.CreateInstance(typeof(R), cdata.ToArray());
                        var cresult = CreateType<R>(cinfo, cdata);
                        //if (id != null)
                        //{
                        ordered_result.Add(new KeyValuePair<int, R>((int)id, cresult));
                        //}
                        //else
                        //{
                        //    var by = data["Id"];
                        //    int k = currents["Id"] + 1;
                        //    byte[] val = new byte[4];
                        //    for (int j = 0; j < 4; j++, k++)
                        //    {
                        //        val[j] = by.Item2[k];
                        //    }
                        //    currents["Id"] += 5;
                        //    id = BitConverter.ToInt32(val.MyReverseNoCopy(), 0);
                        //    ordered_result.Add(new KeyValuePair<int, R>((int)id, cresult));
                        //}
                    }
                    else
                    {
                        //var cresult = (R)Activator.CreateInstance(typeof(R), cdata.ToArray());
                        var cresult = CreateType<R>(cinfo, cdata);
                        result.Add(cresult);
                    }
                }

                if (fres.IsOrdered)
                {
                    result = ordered_result.OrderBy(f => fres.OrderedIds[f.Key]).Select(f => f.Value).ToList();
                }

                return result;
            }
        }

        static List<int> CoerceCtorFloatArgs<T>(List<object> fields, List<int> pos)
        {
            if (pos == null)
            {
                var res = new List<int>();
                var ctor = typeof(T).GetConstructors().First();
                var ps = ctor.GetParameters();

                if (fields.Count != ps.Length)
                    throw new ArgumentException($"Argument count mismatch: got {fields.Count}, expected {ps.Length}.");

                for (int i = 0; i < ps.Length; i++)
                {
                    if (ps[i].ParameterType == typeof(float[]) && fields[i] is byte[])
                    {
                        fields[i] = FloatByteConverter.ByteArrayToFloatArray(fields[i] as byte[]);
                        res.Add(i);
                    }
                }

                return res.Distinct().ToList();
            }
            else if (pos.Any())
            {
                foreach (var p in pos)
                {
                    fields[p] = FloatByteConverter.ByteArrayToFloatArray(fields[p] as byte[]);
                }
                return pos;
            }
            else
            {
                return pos;
            }
        }

        object _cache_lock = new object();
        ConcurrentDictionary<string, List<object>> compiled_cache = new ConcurrentDictionary<string, List<object>>();
        T CreateType<T>(CompiledInfo<T> info, List<object> fields)
        {
            string key = info.ToString();
            if (!compiled_cache.ContainsKey(key))
            {
                lock (_cache_lock)
                {
                    if (!compiled_cache.ContainsKey(key))
                    {
                        compiled_cache[key] = new List<object>() { info.GetActivator() };
                    }
                }
            }
            object item = null;
            for (int i = 0; i < compiled_cache[key].Count(); i++)
            {
                if (compiled_cache[key][i] is CompiledInfo<T>.ObjectActivator)
                {
                    item = compiled_cache[key][i];
                    break;
                }
            }

            if (item == null)
            {
                lock (_cache_lock)
                {
                    item = info.GetActivator();
                    compiled_cache[key].Add(item);
                }
            }

            T instance = (item as CompiledInfo<T>.ObjectActivator)(fields.ToArray());
            return instance;
        }
        //ConcurrentDictionary<string, object> compiled_cache = new ConcurrentDictionary<string, object>();
        //object _ccache_lock = new object();
        //T CreateType<T>(CompiledInfo<T> info, List<object> fields)
        //{
        //    string key = info.ToString();
        //    CompiledInfo<T>.ObjectActivator ob = null;
        //    lock (_ccache_lock)
        //    {
        //        if (!compiled_cache.ContainsKey(key) || !(compiled_cache[key] is CompiledInfo<T>.ObjectActivator))
        //        {
        //            compiled_cache[key] = info.GetActivator(); ;
        //        }
        //        ob = compiled_cache[key] as CompiledInfo<T>.ObjectActivator;
        //    }
        //    //create an instance:
        //    T instance = ob(fields.ToArray());
        //    return instance;
        //}
        Tuple<List<int>, List<byte>> ReadSomeValues(string column_name, TableInfo table_info, OperResult res_set, Dictionary<long, byte[]> cache, ReadOptions ro, ReadByteCount read_counter, int count, int total)
        {
            var index_res = ReadSomeValuesWithIndex(column_name, table_info, res_set, ro, read_counter, count, total);
            if (index_res != null)
            {
                return index_res;
            }

            List<byte> res = null;
            List<int> ids = null;
            if (table_info.Columns[column_name] != LinqDbTypes.binary_ && table_info.Columns[column_name] != LinqDbTypes.vector_ && table_info.Columns[column_name] != LinqDbTypes.string_)
            {
                if (table_info.Columns[column_name] == LinqDbTypes.int_)
                {
                    res = new List<byte>(count * 4 + 5);
                    ids = new List<int>(count + 1);
                }
                else if (table_info.Columns[column_name] == LinqDbTypes.decimal_)
                {
                    res = new List<byte>(count * 32 + 33);
                    ids = new List<int>(count + 1);
                }
                else
                {
                    res = new List<byte>(count * 8 + 5);
                    ids = new List<int>(count + 1);
                }
                foreach (var r in res_set.ResIds)
                {
                    if (column_name == "Id")
                    {
                        var val = BitConverter.GetBytes(r).MyReverseNoCopy(LinqDbTypes.int_);
                        res.Add(1);
                        res.AddRange(val);
                        ids.Add(r);
                    }
                    else
                    {
                        byte[] val = null;
                        //if (!GetFromCache(table_info.ColumnNumbers[column_name], r, cache, out val))
                        //{
                        var key = MakeValueKey(new IndexKeyInfo()
                        {
                            TableNumber = table_info.TableNumber,
                            ColumnNumber = table_info.ColumnNumbers[column_name],
                            Id = r
                        });
                        val = leveld_db.Get(key, null, ro);
                        //}
                        ids.Add(r);
                        if (val == null)
                        {
                            res.Add((byte)0);
                        }
                        else if (ValsEqual(val, NullConstant))
                        {
                            res.Add((byte)250);
                        }
                        else
                        {
                            read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, val.Count() + 1);
                            res.Add(1);
                            res.AddRange(val);
                        }
                    }
                }
            }
            else if (table_info.Columns[column_name] == LinqDbTypes.binary_ || table_info.Columns[column_name] == LinqDbTypes.vector_)
            {
                res = new List<byte>(count * 40);
                ids = new List<int>(count + 1);
                foreach (var r in res_set.ResIds)
                {
                    ids.Add(r);
                    var key = MakeBinaryValueKey(new IndexKeyInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = table_info.ColumnNumbers[column_name],
                        Id = r
                    });
                    var val = leveld_db.Get(key, null, ro);
                    if (val == null)
                    {
                        res.AddRange(BitConverter.GetBytes(-1));
                    }
                    else if (ValsEqual(val, NullConstant))
                    {
                        res.AddRange(BitConverter.GetBytes(-2));
                    }
                    else
                    {
                        read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, val.Count() + 4);
                        res.AddRange(BitConverter.GetBytes(val.Count()));
                        res.AddRange(val);
                    }
                }
            }
            else
            {
                res = new List<byte>(count * 40);
                ids = new List<int>(count + 1);
                foreach (var r in res_set.ResIds)
                {
                    ids.Add(r);
                    var key = MakeStringValueKey(new IndexKeyInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = table_info.ColumnNumbers[column_name],
                        Id = r
                    });
                    var val = leveld_db.Get(key, null, ro);
                    if (val == null)
                    {
                        res.AddRange(BitConverter.GetBytes(-1));
                    }
                    else if (ValsEqual(val, NullConstant))
                    {
                        res.AddRange(BitConverter.GetBytes(-2));
                    }
                    else
                    {
                        read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, val.Count() + 4);
                        res.AddRange(BitConverter.GetBytes(val.Count()));
                        res.AddRange(val);
                    }
                }
            }
            return new Tuple<List<int>, List<byte>>(ids, res);
        }


        Tuple<List<int>, List<byte>> ReadSomeValuesWithIndex(string column_name, TableInfo table_info, OperResult res_set, ReadOptions ro, ReadByteCount read_counter, int count, int total)
        {
            //if (res_set.ResIds.Count() < 100)
            //{
            //    return null;
            //}
            var order_dic = new Dictionary<int, int>(res_set.ResIds != null ? res_set.ResIds.Count() : 0);
            if (res_set.ResIds != null)
            {
                int nr = 0;
                foreach (var rid in res_set.ResIds)
                {
                    order_dic[rid] = nr;
                    nr++;
                }
            }
            if (total == 0)
            {
                return null;
            }
            if (count < 500 && total > 3000000)
            {
                return null;
            }
            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[column_name]);
            var snapid = leveld_db.Get(skey, null, ro);
            if (snapid == null)
            {
                return null;
            }
            var snapshot_id = Encoding.UTF8.GetString(snapid);
            if (string.IsNullOrEmpty(table_info.Name) || string.IsNullOrEmpty(column_name))
            {
                throw new LinqDbException("Linqdb: bad indexes.");
            }
            if (!indexes.ContainsKey(table_info.Name + "|" + column_name + "|" + snapshot_id))
            {
                return null;
            }


            var index = indexes[table_info.Name + "|" + column_name + "|" + snapshot_id];

            skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
            snapid = leveld_db.Get(skey, null, ro);
            var id_snapshot_id = Encoding.UTF8.GetString(snapid);
            var ids_index = indexes[table_info.Name + "|Id|" + id_snapshot_id];
            if (index.IndexType == IndexType.GroupOnly)
            {
                return null;
            }

            switch (table_info.Columns[column_name])
            {
                case LinqDbTypes.int_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, count * 8);
                    List<Tuple<int, int>> res = EfficientContainsOneCall(ids_index, index, res_set.ResIds, LinqDbTypes.int_).Item1;

                    List<int> res_ids = new List<int>(res.Count());
                    List<byte> res_data = new List<byte>(res.Count() * 5);
                    foreach (var val in res.OrderBy(f => order_dic[f.Item1]))
                    {
                        res_ids.Add(val.Item1);
                        res_data.Add((byte)1);
                        res_data.AddRange(BitConverter.GetBytes((int)val.Item2).MyReverseNoCopy(LinqDbTypes.int_));
                    }
                    return new Tuple<List<int>, List<byte>>(res_ids, res_data);
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, count * 12);
                    List<Tuple<int, double>> double_res = EfficientContainsOneCall(ids_index, index, res_set.ResIds, LinqDbTypes.double_).Item2;
                    
                    List<int> dres_ids = new List<int>(double_res.Count());
                    List<byte> dres_data = new List<byte>(double_res.Count() * 9);
                    foreach (var val in double_res.OrderBy(f => order_dic[f.Item1]))
                    {
                        dres_ids.Add(val.Item1);
                        dres_data.Add((byte)1);
                        dres_data.AddRange(BitConverter.GetBytes((double)val.Item2).MyReverseNoCopy(LinqDbTypes.double_));
                    }
                    return new Tuple<List<int>, List<byte>>(dres_ids, dres_data);
                case LinqDbTypes.decimal_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, count * 36);
                    List<Tuple<int, decimal>> decimal_res = EfficientContainsOneCall(ids_index, index, res_set.ResIds, LinqDbTypes.decimal_).Item3;

                    List<int> decres_ids = new List<int>(decimal_res.Count());
                    List<byte> decres_data = new List<byte>(decimal_res.Count() * 33);
                    foreach (var val in decimal_res.OrderBy(f => order_dic[f.Item1]))
                    {
                        decres_ids.Add(val.Item1);
                        decres_data.Add((byte)1);
                        decres_data.AddRange(DecimalConversion.ToByteArray((decimal)val.Item2));
                    }
                    return new Tuple<List<int>, List<byte>>(decres_ids, decres_data);
                case LinqDbTypes.long_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, count * 12);
                    List<Tuple<int, long>> long_res = EfficientContainsOneCall(ids_index, index, res_set.ResIds, LinqDbTypes.long_).Item4;

                    List<int> longres_ids = new List<int>(long_res.Count());
                    List<byte> longres_data = new List<byte>(long_res.Count() * 9);
                    foreach (var val in long_res.OrderBy(f => order_dic[f.Item1]))
                    {
                        longres_ids.Add(val.Item1);
                        longres_data.Add((byte)1);
                        longres_data.AddRange(BitConverter.GetBytes((long)val.Item2).MyReverseNoCopy(LinqDbTypes.long_));
                    }
                    return new Tuple<List<int>, List<byte>>(longres_ids, longres_data);
                default:
                    return null;
            }
        }

        Tuple<List<int>, List<byte>> ReadSomeValuesIterator(string column_name, TableInfo table_info, OperResult res_set, ReadOptions ro, ReadByteCount read_counter, int count, int total)
        {
            var index_res = ReadSomeValuesWithIndex(column_name, table_info, res_set, ro, read_counter, count, total);
            if (index_res != null)
            {
                return index_res;
            }

            List<byte> res = null;
            List<int> ids = null;
            if (table_info.Columns[column_name] != LinqDbTypes.binary_ && table_info.Columns[column_name] != LinqDbTypes.vector_ && table_info.Columns[column_name] != LinqDbTypes.string_)
            {
                if (table_info.Columns[column_name] == LinqDbTypes.int_)
                {
                    res = new List<byte>(count * 4 + 5);
                    ids = new List<int>(count + 1);
                }
                else if (table_info.Columns[column_name] == LinqDbTypes.decimal_)
                {
                    res = new List<byte>(count * 32 + 5);
                    ids = new List<int>(count + 1);
                }
                else
                {
                    res = new List<byte>(count * 8 + 5);
                    ids = new List<int>(count + 1);
                }
                var key = MakeFirstValueKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[column_name]
                });

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var k = it.Key();
                    if (k == null)
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    var info = GetValueKey(k);
                    bool has = false;
                    var contains = new ContainsInfo()
                    {
                        ids = res_set.ResIds
                    };
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        has = EfficientContains(contains, info.Id);
                        if (contains.is_sorted)
                        {
                            res_set.ResIds = contains.ids;
                        }
                        if (has)
                        {
                            ids.Add(info.Id);
                            var valb = it.Value();
                            if (valb == null)
                            {
                                res.Add((byte)0);
                            }
                            else if (ValsEqual(valb, NullConstant))
                            {
                                res.Add((byte)250);
                            }
                            else
                            {
                                read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 1);
                                res.Add(1);
                                res.AddRange(valb);
                            }
                        }
                    }
                    else
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    while (true)
                    {
                        it.Next();
                        if (!it.Valid())
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }

                        k = it.Key();
                        if (k == null)
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                        info = GetValueKey(k);
                        if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                        {
                            has = EfficientContains(contains, info.Id);
                            if (has)
                            {
                                ids.Add(info.Id);
                                var valb = it.Value();
                                if (valb == null)
                                {
                                    res.Add((byte)0);
                                }
                                else if (ValsEqual(valb, NullConstant))
                                {
                                    res.Add((byte)250);
                                }
                                else
                                {
                                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 1);
                                    res.Add(1);
                                    res.AddRange(valb);
                                }
                            }
                        }
                        else
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                    }
                }
            }
            else if (table_info.Columns[column_name] == LinqDbTypes.binary_ || table_info.Columns[column_name] == LinqDbTypes.vector_)
            {
                res = new List<byte>(count * 40);
                ids = new List<int>(count + 1);
                var key = MakeFirstBinaryValueKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[column_name]
                });

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var k = it.Key();
                    if (k == null)
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    var info = GetValueKey(k);
                    bool has = false;
                    var contains = new ContainsInfo()
                    {
                        ids = res_set.ResIds
                    };
                    if (ValsEqual(info.Marker, BinaryValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        has = EfficientContains(contains, info.Id);
                        if (contains.is_sorted)
                        {
                            res_set.ResIds = contains.ids;
                        }
                        if (has)
                        {
                            ids.Add(info.Id);
                            var valb = it.Value();
                            if (valb == null)
                            {
                                res.AddRange(BitConverter.GetBytes(-1));
                            }
                            else if (ValsEqual(valb, NullConstant))
                            {
                                res.AddRange(BitConverter.GetBytes(-2));
                            }
                            else
                            {
                                read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                                res.AddRange(BitConverter.GetBytes(valb.Count()));
                                res.AddRange(valb);
                            }
                        }
                    }
                    else
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    while (true)
                    {
                        it.Next();
                        if (!it.Valid())
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }

                        k = it.Key();
                        if (k == null)
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                        info = GetValueKey(k);
                        if (ValsEqual(info.Marker, BinaryValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                        {
                            has = EfficientContains(contains, info.Id);
                            if (has)
                            {
                                ids.Add(info.Id);
                                var valb = it.Value();
                                if (valb != null && !ValsEqual(valb, NullConstant))
                                {
                                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                                    res.AddRange(BitConverter.GetBytes(valb.Count()));
                                    res.AddRange(valb);
                                }
                                else if (ValsEqual(valb, NullConstant))
                                {
                                    res.AddRange(BitConverter.GetBytes(-2));
                                }
                                else
                                {
                                    res.AddRange(BitConverter.GetBytes(-1));
                                }
                            }
                        }
                        else
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                    }
                }
            }
            else
            {
                res = new List<byte>(count * 40);
                ids = new List<int>(count);
                var key = MakeFirstStringValueKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[column_name]
                });

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var k = it.Key();
                    if (k == null)
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    var info = GetValueKey(k);
                    bool has = false;
                    var contains = new ContainsInfo()
                    {
                        ids = res_set.ResIds
                    };
                    if (ValsEqual(info.Marker, StringValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        has = EfficientContains(contains, info.Id);
                        if (contains.is_sorted)
                        {
                            res_set.ResIds = contains.ids;
                        }
                        if (has)
                        {
                            ids.Add(info.Id);
                            var valb = it.Value();
                            if (valb != null && !ValsEqual(valb, NullConstant))
                            {
                                read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                                res.AddRange(BitConverter.GetBytes(valb.Count()));
                                res.AddRange(valb);
                            }
                            else if (ValsEqual(valb, NullConstant))
                            {
                                res.AddRange(BitConverter.GetBytes(-2));
                            }
                            else
                            {
                                res.AddRange(BitConverter.GetBytes(-1));
                            }
                        }
                    }
                    else
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    while (true)
                    {
                        it.Next();
                        if (!it.Valid())
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }

                        k = it.Key();
                        if (k == null)
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                        info = GetValueKey(k);
                        if (ValsEqual(info.Marker, StringValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                        {
                            has = EfficientContains(contains, info.Id);
                            if (has)
                            {
                                ids.Add(info.Id);
                                var valb = it.Value();
                                if (valb != null && !ValsEqual(valb, NullConstant))
                                {
                                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                                    res.AddRange(BitConverter.GetBytes(valb.Count()));
                                    res.AddRange(valb);
                                }
                                else if (ValsEqual(valb, NullConstant))
                                {
                                    res.AddRange(BitConverter.GetBytes(-2));
                                }
                                else
                                {
                                    res.AddRange(BitConverter.GetBytes(-1));
                                }
                            }
                        }
                        else
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                    }
                }
            }
        }

        public int AddReadBytes(int cv, int add)
        {
            cv += add;
            if (cv > MaxSelectByteCount)
            {
                throw new LinqDbException("Linqdb: the query resulted in set larger than " + MaxSelectByteCount + " bytes, which is a bad practice, resulting in OutOfMemoryException. Select in smaller batches or use non-atomic select.");
            }
            return cv;
        }

        List<int> ReadAllIds(TableInfo table_info, ReadOptions ro, int total)
        {
            var res = new List<int>(total);

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers["Id"]
            });
            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return new List<int>();
                }

                var k = it.Key();
                if (k == null)
                {
                    return new List<int>();
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers["Id"])
                {
                    res.Add(info.Id);
                }
                else
                {
                    return new List<int>();
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }
                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers["Id"])
                    {
                        res.Add(info.Id);
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }
        List<int> ReadAllIntValuesList(string column_name, TableInfo table_info, ReadOptions ro, int max_id, out int total)
        {
            total = 0;
            var res = new List<int>(max_id + 1);
            for (int i = 0; i < max_id + 1; i++)
            {
                res.Add(0);
            }

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    total++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = BitConverter.ToInt32(new byte[4] { valb[3], valb[2], valb[1], valb[0] }, 0);
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }
                
                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        total++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = BitConverter.ToInt32(new byte[4] { valb[3], valb[2], valb[1], valb[0] }, 0);
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        Dictionary<int, int> ReadAllIntValuesDic(string column_name, TableInfo table_info, ReadOptions ro, int max_id, int total, out int totalex)
        {
            totalex = 0;
            var res = new Dictionary<int, int>(total);

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    totalex++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = BitConverter.ToInt32(new byte[4] { valb[3], valb[2], valb[1], valb[0] }, 0);
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        totalex++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = BitConverter.ToInt32(new byte[4] { valb[3], valb[2], valb[1], valb[0] }, 0);
                        }
                        else
                        {
                            throw new LinqDbException("Null values are not supported in indexes.");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        List<double> ReadAllDoubleValuesList(string column_name, TableInfo table_info, ReadOptions ro, int max_id, out int total)
        {
            total = 0;
            var res = new List<double>(max_id + 1);
            for (int i = 0; i < max_id + 1; i++)
            {
                res.Add(0);
            }

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    total++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = BitConverter.ToDouble(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0]}, 0);
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        total++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = BitConverter.ToDouble(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        List<decimal> ReadAllDecimalValuesList(string column_name, TableInfo table_info, ReadOptions ro, int max_id, out int total)
        {
            total = 0;
            var res = new List<decimal>(max_id + 1);
            for (int i = 0; i < max_id + 1; i++)
            {
                res.Add(0);
            }

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    total++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = DecimalConversion.FromByteArray(new byte[32] { valb[0], valb[1], valb[2], valb[3], valb[4], valb[5], valb[6], valb[7], valb[8], valb[9], valb[10], valb[11], valb[12], valb[13], valb[14], valb[15], valb[16], valb[17], valb[18], valb[19], valb[20], valb[21], valb[22], valb[23], valb[24], valb[25], valb[26], valb[27], valb[28], valb[29], valb[30], valb[31] });
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        total++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = DecimalConversion.FromByteArray(new byte[32] { valb[0], valb[1], valb[2], valb[3], valb[4], valb[5], valb[6], valb[7], valb[8], valb[9], valb[10], valb[11], valb[12], valb[13], valb[14], valb[15], valb[16], valb[17], valb[18], valb[19], valb[20], valb[21], valb[22], valb[23], valb[24], valb[25], valb[26], valb[27], valb[28], valb[29], valb[30], valb[31] });
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        List<long> ReadAllLongValuesList(string column_name, TableInfo table_info, ReadOptions ro, int max_id, out int total)
        {
            total = 0;
            var res = new List<long>(max_id + 1);
            for (int i = 0; i < max_id + 1; i++)
            {
                res.Add(0);
            }

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    total++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = BitConverter.ToInt64(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        total++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = BitConverter.ToInt64(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        //List<string> ReadAllStringValuesList(string column_name, TableInfo table_info, ReadOptions ro, int max_id, out int total)
        //{
        //    total = 0;
        //    var res = new List<string>(max_id + 1);
        //    for (int i = 0; i < max_id + 1; i++)
        //    {
        //        res.Add(string.Empty);
        //    }

        //    var key = MakeFirstStringValueKey(new IndexKeyInfo()
        //    {
        //        TableNumber = table_info.TableNumber,
        //        ColumnNumber = table_info.ColumnNumbers[column_name]
        //    });

        //    using (var it = leveld_db.NewIterator(null, ro))
        //    {
        //        it.Seek(key);
        //        if (!it.Valid())
        //        {
        //            return res;
        //        }

        //        var k = it.Key();
        //        if (k == null)
        //        {
        //            return res;
        //        }
        //        var info = GetValueKey(k);
        //        if (ValsEqual(info.Marker, StringValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
        //        {
        //            var valb = it.Value();
        //            total++;
        //            if (valb != null && !ValsEqual(valb, NullConstant))
        //            {
        //                res[info.Id] = Encoding.Unicode.GetString(valb.Skip(1).ToArray());
        //            }
        //            else
        //            {
        //                res[info.Id] = string.Empty;
        //            }
        //        }
        //        else
        //        {
        //            return res;
        //        }

        //        while (true)
        //        {
        //            it.Next();
        //            if (!it.Valid())
        //            {
        //                return res;
        //            }

        //            k = it.Key();
        //            if (k == null)
        //            {
        //                return res;
        //            }
        //            info = GetValueKey(k);
        //            if (ValsEqual(info.Marker, StringValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
        //            {
        //                var valb = it.Value();
        //                total++;
        //                if (valb != null && !ValsEqual(valb, NullConstant))
        //                {
        //                    res[info.Id] = Encoding.Unicode.GetString(valb.Skip(1).ToArray());
        //                }
        //                else
        //                {
        //                    res[info.Id] = string.Empty;
        //                }
        //            }
        //            else
        //            {
        //                return res;
        //            }
        //        }
        //    }
        //}

        IEnumerable<List<Tuple<int, string>>> ReadAllStringValuesList(string column_name, TableInfo table_info, ReadOptions ro, int batch_size)
        {
            var batch = new List<Tuple<int, string>>(batch_size);

            var key = MakeFirstStringValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                while (it.Valid())
                {
                    var k = it.Key();
                    if (k == null) break;

                    var info = GetValueKey(k);
                    if (!(ValsEqual(info.Marker, StringValueKeyStart) &&
                          info.TableNumber == table_info.TableNumber &&
                          info.ColumnNumber == table_info.ColumnNumbers[column_name]))
                    {
                        break;
                    }

                    var valb = it.Value();
                    var value = (valb != null && !ValsEqual(valb, NullConstant))
                        ? Encoding.Unicode.GetString(valb.Skip(1).ToArray())
                        : string.Empty;

                    batch.Add(new Tuple<int, string>(info.Id, value));
                    if (batch.Count == batch_size)
                    {
                        yield return batch;
                        batch = new List<Tuple<int, string>>(batch_size);
                    }

                    it.Next();
                }
            }

            if (batch.Count > 0)
            {
                yield return batch;
            }
        }


        Dictionary<int, double> ReadAllDoubleValuesDic(string column_name, TableInfo table_info, ReadOptions ro, int max_id, int total, out int totalex)
        {
            totalex = 0;
            var res = new Dictionary<int, double>(total);

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    totalex++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = BitConverter.ToDouble(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        totalex++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = BitConverter.ToDouble(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        Dictionary<int, decimal> ReadAllDecimalValuesDic(string column_name, TableInfo table_info, ReadOptions ro, int max_id, int total, out int totalex)
        {
            totalex = 0;
            var res = new Dictionary<int, decimal>(total);

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    totalex++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = DecimalConversion.FromByteArray(new byte[32] { valb[0], valb[1], valb[2], valb[3], valb[4], valb[5], valb[6], valb[7], valb[8], valb[9], valb[10], valb[11], valb[12], valb[13], valb[14], valb[15], valb[16], valb[17], valb[18], valb[19], valb[20], valb[21], valb[22], valb[23], valb[24], valb[25], valb[26], valb[27], valb[28], valb[29], valb[30], valb[31] });
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        totalex++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = DecimalConversion.FromByteArray(new byte[32] { valb[0], valb[1], valb[2], valb[3], valb[4], valb[5], valb[6], valb[7], valb[8], valb[9], valb[10], valb[11], valb[12], valb[13], valb[14], valb[15], valb[16], valb[17], valb[18], valb[19], valb[20], valb[21], valb[22], valb[23], valb[24], valb[25], valb[26], valb[27], valb[28], valb[29], valb[30], valb[31] });
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        Dictionary<int, long> ReadAllLongValuesDic(string column_name, TableInfo table_info, ReadOptions ro, int max_id, int total, out int totalex)
        {
            totalex = 0;
            var res = new Dictionary<int, long>(total);

            var key = MakeFirstValueKey(new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[column_name]
            });

            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetValueKey(k);
                if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                {
                    var valb = it.Value();
                    totalex++;
                    if (valb != null && !ValsEqual(valb, NullConstant))
                    {
                        res[info.Id] = BitConverter.ToInt64(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                    }
                    else
                    {
                        throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                    }
                }
                else
                {
                    return res;
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        var valb = it.Value();
                        totalex++;
                        if (valb != null && !ValsEqual(valb, NullConstant))
                        {
                            res[info.Id] = BitConverter.ToInt64(new byte[8] { valb[7], valb[6], valb[5], valb[4], valb[3], valb[2], valb[1], valb[0] }, 0);
                        }
                        else
                        {
                            throw new LinqDbException($"Linqdb: in-memory indexes do not support null values. Column name {column_name}");
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        Tuple<List<int>, List<byte>> ReadAllValues(string column_name, TableInfo table_info, OperResult res_set, ReadOptions ro, ReadByteCount read_counter, int total)
        {
            var index_res = ReadAllValuesWithIndex(column_name, table_info, res_set, ro, read_counter, total);
            if (index_res != null)
            {
                return index_res;
            }

            List<byte> res = null;
            List<int> ids = null;
            if (table_info.Columns[column_name] != LinqDbTypes.binary_ && table_info.Columns[column_name] != LinqDbTypes.vector_ && table_info.Columns[column_name] != LinqDbTypes.string_)
            {
                if (table_info.Columns[column_name] == LinqDbTypes.int_)
                {
                    res = new List<byte>(total * 4 + 5);
                    ids = new List<int>(total + 1);
                }
                else if (table_info.Columns[column_name] == LinqDbTypes.decimal_)
                {
                    res = new List<byte>(total * 32 + 5);
                    ids = new List<int>(total + 1);
                }
                else
                {
                    res = new List<byte>(total * 8 + 5);
                    ids = new List<int>(total + 1);
                }
                var key = MakeFirstValueKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[column_name]
                });

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var k = it.Key();
                    if (k == null)
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    var info = GetValueKey(k);
                    if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        ids.Add(info.Id);
                        var valb = it.Value();
                        if (valb == null)
                        {
                            res.Add((byte)0);
                        }
                        else if (ValsEqual(valb, NullConstant))
                        {
                            res.Add((byte)250);
                        }
                        else
                        {
                            read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 1);
                            res.Add(1);
                            res.AddRange(valb);
                        }
                    }
                    else
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var rg = new Random();
                    while (true)
                    {
                        it.Next();
                        if (!it.Valid())
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }

                        k = it.Key();
                        if (k == null)
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                        info = GetValueKey(k);
                        if (ValsEqual(info.Marker, ValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                        {
                            ids.Add(info.Id);
                            var valb = it.Value();
                            if (valb == null)
                            {
                                res.Add((byte)0);
                            }
                            else if (ValsEqual(valb, NullConstant))
                            {
                                res.Add((byte)250);
                            }
                            else
                            {
                                read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 1);
                                res.Add(1);
                                res.AddRange(valb);
                            }
                        }
                        else
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                    }
                }
            }
            else if (table_info.Columns[column_name] == LinqDbTypes.binary_ || table_info.Columns[column_name] == LinqDbTypes.vector_)
            {
                res = new List<byte>(total * 40);
                ids = new List<int>(total);
                var key = MakeFirstBinaryValueKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[column_name]
                });

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var k = it.Key();
                    if (k == null)
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    var info = GetValueKey(k);
                    if (ValsEqual(info.Marker, BinaryValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        ids.Add(info.Id);
                        var valb = it.Value();
                        if (valb == null)
                        {
                            res.AddRange(BitConverter.GetBytes(-1));
                        }
                        else if (ValsEqual(valb, NullConstant))
                        {
                            res.AddRange(BitConverter.GetBytes(-2));
                        }
                        else
                        {
                            read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                            res.AddRange(BitConverter.GetBytes(valb.Count()));
                            res.AddRange(valb);
                        }
                    }
                    else
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    while (true)
                    {
                        it.Next();
                        if (!it.Valid())
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }

                        k = it.Key();
                        if (k == null)
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                        info = GetValueKey(k);
                        if (ValsEqual(info.Marker, BinaryValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                        {
                            ids.Add(info.Id);
                            var valb = it.Value();
                            if (valb == null)
                            {
                                res.AddRange(BitConverter.GetBytes(-1));
                            }
                            else if (ValsEqual(valb, NullConstant))
                            {
                                res.AddRange(BitConverter.GetBytes(-2));
                            }
                            else
                            {
                                read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                                res.AddRange(BitConverter.GetBytes(valb.Count()));
                                res.AddRange(valb);
                            }
                        }
                        else
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                    }
                }
            }
            else
            {
                res = new List<byte>(total * 40);
                ids = new List<int>(total + 1);
                var key = MakeFirstStringValueKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[column_name]
                });

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }

                    var k = it.Key();
                    if (k == null)
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    var info = GetValueKey(k);
                    if (ValsEqual(info.Marker, StringValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                    {
                        ids.Add(info.Id);
                        var valb = it.Value();
                        if (valb == null)
                        {
                            res.AddRange(BitConverter.GetBytes(-1));
                        }
                        else if (ValsEqual(valb, NullConstant))
                        {
                            res.AddRange(BitConverter.GetBytes(-2));
                        }
                        else
                        {
                            read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                            res.AddRange(BitConverter.GetBytes(valb.Count()));
                            res.AddRange(valb);
                        }
                    }
                    else
                    {
                        return new Tuple<List<int>, List<byte>>(ids, res);
                    }
                    while (true)
                    {
                        it.Next();
                        if (!it.Valid())
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }

                        k = it.Key();
                        if (k == null)
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                        info = GetValueKey(k);
                        if (ValsEqual(info.Marker, StringValueKeyStart) && info.TableNumber == table_info.TableNumber && info.ColumnNumber == table_info.ColumnNumbers[column_name])
                        {
                            ids.Add(info.Id);
                            var valb = it.Value();
                            if (valb == null)
                            {
                                res.AddRange(BitConverter.GetBytes(-1));
                            }
                            else if (ValsEqual(valb, NullConstant))
                            {
                                res.AddRange(BitConverter.GetBytes(-2));
                            }
                            else
                            {
                                read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, valb.Count() + 4);
                                res.AddRange(BitConverter.GetBytes(valb.Count()));
                                res.AddRange(valb);
                            }
                        }
                        else
                        {
                            return new Tuple<List<int>, List<byte>>(ids, res);
                        }
                    }
                }
            }
        }

        Tuple<List<int>, List<byte>> ReadAllValuesWithIndex(string column_name, TableInfo table_info, OperResult res_set, ReadOptions ro, ReadByteCount read_counter, int total)
        {
            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[column_name]);
            var snapid = leveld_db.Get(skey, null, ro);
            if (snapid == null)
            {
                return null;
            }
            var snapshot_id = Encoding.UTF8.GetString(snapid);
            if (string.IsNullOrEmpty(table_info.Name) || string.IsNullOrEmpty(column_name))
            {
                throw new LinqDbException("Linqdb: bad indexes.");
            }
            if (!indexes.ContainsKey(table_info.Name + "|" + column_name + "|" + snapshot_id))
            {
                return null;
            }
            var index = indexes[table_info.Name + "|" + column_name + "|" + snapshot_id];

            skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
            snapid = leveld_db.Get(skey, null, ro);
            var id_snapshot_id = Encoding.UTF8.GetString(snapid);
            var ids_index = indexes[table_info.Name + "|Id|" + id_snapshot_id];

            if (index.IndexType == IndexType.GroupOnly)
            {
                return null;
            }

            switch (table_info.Columns[column_name])
            {
                case LinqDbTypes.int_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, total * 8);
                    List<Tuple<int, int?>> res = new List<Tuple<int, int?>>(total);
                    int icount = index.Parts.Count();
                    for (int i = 0; i < icount; i++)
                    {
                        var ids = ids_index.Parts[i].IntValues;
                        var iv = index.Parts[i].IntValues;
                        int jcount = iv.Count();
                        for (int j = 0; j < jcount; j++)
                        {
                            int id = (int)ids[j];
                            res.Add(new Tuple<int, int?>(id, iv[j]));
                        }
                    }
                    List<int> res_ids = new List<int>(res.Count());
                    List<byte> res_data = new List<byte>(res.Count() * 5);
                    foreach (var val in res.OrderBy(f => f.Item1))
                    {
                        res_ids.Add(val.Item1);
                        if (val.Item2 == null)
                        {
                            res_data.Add((byte)250);
                        }
                        else
                        {
                            res_data.Add((byte)1);
                            res_data.AddRange(BitConverter.GetBytes((int)val.Item2).MyReverseNoCopy(LinqDbTypes.int_));
                        }
                    }
                    return new Tuple<List<int>, List<byte>>(res_ids, res_data);
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, total * 12);
                    List<Tuple<int, double?>> double_res = new List<Tuple<int, double?>>(total);
                    int dcount = index.Parts.Count();
                    for (int i = 0; i < dcount; i++)
                    {
                        var ids = ids_index.Parts[i].IntValues;
                        var iv = index.Parts[i].DoubleValues;
                        int jcount = iv.Count();
                        for (int j = 0; j < jcount; j++)
                        {
                            int id = (int)ids[j];
                            double_res.Add(new Tuple<int, double?>(id, iv[j]));
                        }
                    }
                    List<int> dres_ids = new List<int>(double_res.Count());
                    List<byte> dres_data = new List<byte>(double_res.Count() * 9);
                    foreach (var val in double_res.OrderBy(f => f.Item1))
                    {
                        dres_ids.Add(val.Item1);
                        if (val.Item2 == null)
                        {
                            dres_data.Add((byte)250);
                        }
                        else
                        {
                            dres_data.Add((byte)1);
                            dres_data.AddRange(BitConverter.GetBytes((double)val.Item2).MyReverseNoCopy(LinqDbTypes.double_));
                        }
                    }
                    return new Tuple<List<int>, List<byte>>(dres_ids, dres_data);
                case LinqDbTypes.decimal_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, total * 36);
                    List<Tuple<int, decimal?>> decimal_res = new List<Tuple<int, decimal?>>(total);
                    int dcountdec = index.Parts.Count();
                    for (int i = 0; i < dcountdec; i++)
                    {
                        var ids = ids_index.Parts[i].IntValues;
                        var iv = index.Parts[i].DecimalValues;
                        int jcount = iv.Count();
                        for (int j = 0; j < jcount; j++)
                        {
                            int id = (int)ids[j];
                            decimal_res.Add(new Tuple<int, decimal?>(id, iv[j]));
                        }
                    }
                    List<int> decres_ids = new List<int>(decimal_res.Count());
                    List<byte> decres_data = new List<byte>(decimal_res.Count() * 33);
                    foreach (var val in decimal_res.OrderBy(f => f.Item1))
                    {
                        decres_ids.Add(val.Item1);
                        if (val.Item2 == null)
                        {
                            decres_data.Add((byte)250);
                        }
                        else
                        {
                            decres_data.Add((byte)1);
                            decres_data.AddRange(DecimalConversion.ToByteArray((decimal)val.Item2));
                        }
                    }
                    return new Tuple<List<int>, List<byte>>(decres_ids, decres_data);
                case LinqDbTypes.long_:
                    read_counter.read_size_bytes = AddReadBytes(read_counter.read_size_bytes, total * 12);
                    List<Tuple<int, long?>> long_res = new List<Tuple<int, long?>>(total);
                    int lcountlong = index.Parts.Count();
                    for (int i = 0; i < lcountlong; i++)
                    {
                        var ids = ids_index.Parts[i].IntValues;
                        var lv = index.Parts[i].LongValues;
                        int jcount = lv.Count();
                        for (int j = 0; j < jcount; j++)
                        {
                            int id = (int)ids[j];
                            long_res.Add(new Tuple<int, long?>(id, lv[j]));
                        }
                    }
                    List<int> longres_ids = new List<int>(long_res.Count());
                    List<byte> longres_data = new List<byte>(long_res.Count() * 9);
                    foreach (var val in long_res.OrderBy(f => f.Item1))
                    {
                        longres_ids.Add(val.Item1);
                        if (val.Item2 == null)
                        {
                            longres_data.Add((byte)250);
                        }
                        else
                        {
                            longres_data.Add((byte)1);
                            longres_data.AddRange(BitConverter.GetBytes((long)val.Item2));
                        }
                    }
                    return new Tuple<List<int>, List<byte>>(longres_ids, longres_data);
                default:
                    return null;
            }
        }


        List<OperResult> CalculateWhereResult<T>(QueryTree tree, TableInfo table_info, ReadOptions ro)
        {
            var res_list = new List<OperResult>();
            if (tree.WhereInfo == null)
            {
                return new List<OperResult>() { new OperResult() { All = true } };
            }

            //OperResult current = null;
            foreach (var st in tree.WhereInfo)
            {
                var res = CalculateOneWhereResult<T>(st, table_info, tree.QueryCache, ro);
                res.Id = st.Id;
                res.OrWith = st.OrWith;
                res_list.Add(res);
            }

            return res_list;
        }
        //void PutToCache(IndexKeyInfo kinfo, Dictionary<long, byte[]> cache)
        //{
        //    long key = kinfo.Id * 100000 + kinfo.ColumnNumber;
        //    cache[key] = kinfo.Val;
        //}
        //bool GetFromCache(short column_number, int id, Dictionary<long, byte[]> cache, out byte[] value)
        //{
        //    long key = id * 100000 + column_number;
        //    if (!cache.ContainsKey(key))
        //    {
        //        value = null;
        //        return false;
        //    }
        //    else
        //    {
        //        value = cache[key];
        //        return true;
        //    }
        //}

        //useful when saving batch data
        void PutToStringIndexCache(IndexKeyInfo kinfo, string word, byte[] bin_key, HashSet<int> value, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> cache, int phase, int is_add)
        {
            string key = phase + ":" + is_add + ":" + kinfo.TableNumber + ":" + kinfo.ColumnNumber + ":" + word;
            cache[key] = new KeyValuePair<byte[], HashSet<int>>(bin_key, value);
        }
        bool GetFromStringIndexCache(IndexKeyInfo kinfo, string word, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> cache, int phase, out HashSet<int> value, int is_add)
        {
            string key = phase + ":" + is_add + ":" + kinfo.TableNumber + ":" + kinfo.ColumnNumber + ":" + word;
            if (!cache.ContainsKey(key))
            {
                value = new HashSet<int>();
                return false;
            }
            else
            {
                value = cache[key].Value;
                return true;
            }
        }

        OperResult CalculateOneWhereResult<T>(WhereInfo info, TableInfo table_info, Dictionary<long, byte[]> cache, ReadOptions ro)
        {
            var wstack = info.Opers;
            var nstack = new Stack<Oper>();
            while (wstack.Any())
            {
                var op = wstack.Pop();
                if (!op.IsOperator)
                {
                    nstack.Push(op);
                    continue;
                }
                else
                {
                    var o1 = nstack.Pop();
                    var o2 = nstack.Pop();
                    if (o1.IsResult && !o2.IsResult || !o1.IsResult && o2.IsResult)
                    {
                        throw new LinqDbException("Linqdb: error in Where statement, probably field selector is used in the expression. That's not supported, i.e. .Where(f => f.Value % 2 == 0) won't work.");
                    }
                    var op_res = new Oper()
                    {
                        IsResult = true
                    };
                    if (!o1.IsResult)
                    {
                        var odb = o1.IsDb ? o1 : o2;
                        object v = o1.IsDb ? o2.NonDbValue : o1.NonDbValue;
                        var val = EncodeValue(odb.ColumnType, v);

                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[odb.ColumnName]);
                        var snapid = leveld_db.Get(skey, null, ro);
                        string snapshot_id = null;
                        if (snapid != null)
                        {
                            snapshot_id = Encoding.UTF8.GetString(snapid);
                        }

                        string id_snapshot_id = null;
                        skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
                        snapid = leveld_db.Get(skey, null, ro);
                        if (snapid != null)
                        {
                            id_snapshot_id = Encoding.UTF8.GetString(snapid);
                        }

                        switch (op.Type)
                        {
                            case ExpressionType.Equal:
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = EqualOperator(odb, val, table_info, cache, ro, snapshot_id, id_snapshot_id)
                                };
                                break;
                            case ExpressionType.LessThan:
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = LessThanOperator(odb, val, false, cache, ro, snapshot_id, id_snapshot_id)
                                };
                                odb.ColumnNumber *= -1;
                                var negl = LessThanNegativeOperator(odb, val, false, cache, ro, snapshot_id, id_snapshot_id);
                                op_res.Result.ResIds = op_res.Result.ResIds.MyUnion(negl);
                                break;
                            case ExpressionType.GreaterThan:
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = GreaterThanOperator(odb, val, false, cache, ro, snapshot_id, id_snapshot_id)
                                };
                                odb.ColumnNumber *= -1;
                                var negg = GreaterThanNegativeOperator(odb, val, false, cache, ro, snapshot_id, id_snapshot_id);
                                op_res.Result.ResIds = op_res.Result.ResIds.MyUnion(negg);
                                break;
                            case ExpressionType.LessThanOrEqual:
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = LessThanOperator(odb, val, true, cache, ro, snapshot_id, id_snapshot_id)
                                };
                                odb.ColumnNumber *= -1;
                                var nege = LessThanNegativeOperator(odb, val, true, cache, ro, snapshot_id, id_snapshot_id);
                                op_res.Result.ResIds = op_res.Result.ResIds.MyUnion(nege);
                                break;
                            case ExpressionType.GreaterThanOrEqual:
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = GreaterThanOperator(odb, val, true, cache, ro, snapshot_id, id_snapshot_id)
                                };
                                odb.ColumnNumber *= -1;
                                var negge = GreaterThanNegativeOperator(odb, val, true, cache, ro, snapshot_id, id_snapshot_id);
                                op_res.Result.ResIds = op_res.Result.ResIds.MyUnion(negge);
                                break;
                            case ExpressionType.NotEqual:
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = NotEqualOperator(odb, val, cache, ro, snapshot_id, id_snapshot_id)
                                };
                                break;
                            default:
                                break;
                        }
                    }
                    else
                    {
                        switch (op.Type)
                        {
                            case ExpressionType.AndAlso:
                                o1.Result.ResIds  = o1.Result.ResIds.MyIntersect(o2.Result.ResIds);
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = o1.Result.ResIds
                                };
                                break;
                            case ExpressionType.OrElse:
                                o1.Result.ResIds = o1.Result.ResIds.MyUnion(o2.Result.ResIds);
                                op_res.IsResult = true;
                                op_res.Result = new OperResult()
                                {
                                    ResIds = o1.Result.ResIds
                                };
                                break;
                            default:
                                throw new LinqDbException("Linqdb: Only && and || operators are supported in where clause.");
                        }
                    }
                    nstack.Push(op_res);
                }
            }

            return nstack.Pop().Result;
        }
    }

    public class CompiledInfo<T>
    {
        public delegate T ObjectActivator(params object[] args);
        public ObjectActivator Compiled
        {
            get;
            set;
        }
        public ObjectActivator GetActivator()
        {
            if (Compiled != null)
            {
                return Compiled;
            }
            var ctor = typeof(T).GetConstructors().First();
            Type type = ctor.DeclaringType;
            ParameterInfo[] paramsInfo = ctor.GetParameters();

            //create a single param of type object[]
            ParameterExpression param = Expression.Parameter(typeof(object[]), "args");

            Expression[] argsExp = new Expression[paramsInfo.Length];

            //pick each arg from the params array 
            //and create a typed expression of them
            for (int i = 0; i < paramsInfo.Length; i++)
            {
                Expression index = Expression.Constant(i);
                Type paramType = paramsInfo[i].ParameterType;

                Expression paramAccessorExp = Expression.ArrayIndex(param, index);

                Expression paramCastExp = Expression.Convert(paramAccessorExp, paramType);

                argsExp[i] = paramCastExp;
            }

            //make a NewExpression that calls the
            //ctor with the args we just created
            NewExpression newExp = Expression.New(ctor, argsExp);

            //create a lambda with the New
            //Expression as body and our param object[] as arg
            LambdaExpression lambda = Expression.Lambda(typeof(ObjectActivator), newExp, param);

            //compile it
            ObjectActivator compiled = (ObjectActivator)lambda.Compile();
            Compiled = compiled;
            return compiled;
        }
    }
}

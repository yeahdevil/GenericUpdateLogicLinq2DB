using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using LinqToDB;
using LinqToDB.Configuration;
using LinqToDB.Data;
using LinqToDB.Linq;
using LinqToDB.Mapping;

namespace POCAllTableUpdateLogic
{
    public class Input
    {
        public string Entity { get; set; }
        public Dictionary<string, string> pairs { get; set; }
        public Condition condition { get; set; }
    }
    public class Condition
    {
        public string primaryKeyVal { get; set; }
        public string primaryKey { get; set; }
    }

    public class Program
    {
        public const string queryMain = "Update {0} Set {1} Where {2}";
        public const string conquery = " {0} = '{1}' ";
        static void Main(string[] args)
        {
            //DataConnection.DefaultSettings = new MySettings();
            var pairs = new Dictionary<string, string>();
            var input = new Input { Entity = "Entity", pairs = pairs, condition = new Condition { primaryKeyVal = "val", primaryKey = "ID" } };
            string tblName = string.Empty;
            string primaryClmName = string.Empty;
            Dictionary<string, string> columns = new Dictionary<string, string>();
            try
            {
                Assembly SampleAssembly;
                SampleAssembly = Assembly.LoadFrom("Assembly");
                Type type = SampleAssembly.GetTypes().Where(t => t.IsClass == true && t.Name == input.Entity).FirstOrDefault();
                List<MemberInfo> columnsProperties= new List<MemberInfo>();
                //Step 1 Get TableName
                var classAttribute = type.GetCustomAttribute<TableAttribute>();
                if (classAttribute == null) return;
                tblName = classAttribute.Name;

                //Step 2 Get ColumnName
                foreach (var pair in input.pairs)
                {
                    var property = type.GetProperty(pair.Key);
                    if (property == null) return;
                    var propAttribute = property.GetCustomAttribute<ColumnAttribute>();
                    if (propAttribute == null) return;
                    columns.Add(propAttribute.Name, pair.Value);
                    columnsProperties.Add(type.GetMember(pair.Key).FirstOrDefault());
                }
                //Step 3 Get primary key
                var primaryKeyProperty = type.GetProperty(input.condition.primaryKey);
                primaryClmName = primaryKeyProperty.GetCustomAttribute<ColumnAttribute>()?.Name;

                //Step4 RunQuery
                #region StringBulder
                if (!string.IsNullOrEmpty(tblName) && !string.IsNullOrEmpty(primaryClmName) && columns.Count > 0)
                {
                    StringBuilder setConditions = new StringBuilder();
                    var flag = true;
                    foreach (var column in columns)
                    {
                        if (!flag)
                            setConditions.Append(",");

                        setConditions.AppendFormat(conquery, column.Key, column.Value);
                        flag = false;
                    }
                    StringBuilder whereConditions = new StringBuilder();
                    whereConditions.AppendFormat(conquery, primaryClmName, input.condition.primaryKeyVal);

                    StringBuilder query = new StringBuilder();
                    query.AppendFormat(queryMain, tblName, setConditions, whereConditions);
                    using (var DataConnection = new LinqToDB.Data.DataConnection(LinqToDB.ProviderName.SqlServer2012, "ConnectionString"))
                    {

                        DataConnection.Execute(query.ToString());
                    }
                    #endregion
                    //Class creation
                    var obj = Activator.CreateInstance(type);
                    foreach(var propertycolmn in columnsProperties)
                    {
                        //obj.GetType().GetProperty(propertycolmn.Name).SetValue(obj, Convert.ChangeType("Disc", propertycolmn.PropertyType));
                    }
                    Type ctype = typeof(RunQuery<>);
                    Type c = ctype.MakeGenericType(type);
                    var conn = Activator.CreateInstance(c, type, primaryKeyProperty, columnsProperties, obj);
                    
                }
                Console.WriteLine("END...");
            }
            catch (Exception ex)
            {
                Console.Write(ex.Message);
            }
        }
    }

    public class RunQuery<T> 
    {
        public RunQuery(Type type, PropertyInfo primaryKeyProperty, List<MemberInfo> columnsProperties, T obj)
        {
            var DataConnection = new LinqToDB.Data.DataConnection(LinqToDB.ProviderName.SqlServer2012, "connectionString");
            var t = typeof(DataConnection);
            var methodInfo = t.GetMethods().Where(m => m.Name == "GetTable" && m.GetParameters().Count() == 0).FirstOrDefault();
            var genericmethod = methodInfo.MakeGenericMethod(type);
            Table = (ITable<T>)genericmethod.Invoke(DataConnection, null);

            var exp = PropertyEquals<T>(primaryKeyProperty, Val);
            var data = Table.Where(exp).FirstOrDefault();
            var object1 = BuildLambda();

            using (var trans = DataConnection.BeginTransaction(IsolationLevel.ReadUncommitted))
            {

                var kak = Table.Where(exp).Update(object1);
                trans.Commit();
            }
            
        }
        public Expression<Func<T, T>> BuildLambda()
        {
            var createdType = typeof(T);
            var displayValueParam = Expression.Parameter(typeof(T), "x");
            var ctor = Expression.New(createdType);
            var displayValueProperty = createdType.GetProperty("Key");
            var displayValueAssignment = Expression.Bind(
                displayValueProperty, Expression.Constant("Val"));
            var memberInit = Expression.MemberInit(ctor, displayValueAssignment);

            return
                Expression.Lambda<Func<T, T>>(memberInit, displayValueParam);
        }
        public Expression<Func<T, bool>> PropertyEquals<T>(PropertyInfo property, int value)
        {
            var param = Expression.Parameter(typeof(T));
            var body = Expression.Equal(Expression.Property(param, property),
                Expression.Constant(value));
            return Expression.Lambda<Func<T, bool>>(body, param);
        }

        public T PropertySet<T>(List<PropertyInfo> properties, T value)
        {
            foreach (var property in properties)
            {
                property.SetValue(value, "Prop");
            }//Expression conversion = Expression.Lambda<Func<T, string>>(body, param);
            return value;
        }
        private ITable<T> Table { get; set; }

    }

}

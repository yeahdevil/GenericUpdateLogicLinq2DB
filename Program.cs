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
        static void Main(string[] args)
        {
            //DataConnection.DefaultSettings = new MySettings();
            var pairs = new Dictionary<string, string>();
            pairs.Add("Col1", "Val1");
            pairs.Add("Col2", "Val2");
            var input = new Input { Entity = "Entity", pairs = pairs, condition = new Condition { primaryKeyVal = "Val", primaryKey = "Key" } };
            try
            {
                Assembly SampleAssembly;
                SampleAssembly = Assembly.LoadFrom("__AssembelyLink");
                Type type = SampleAssembly.GetTypes().Where(t => t.IsClass == true && t.Name == input.Entity).FirstOrDefault();

                Type ctype = typeof(RunQuery<>);
                Type c = ctype.MakeGenericType(type);
                Activator.CreateInstance(c, type, input);


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
        public RunQuery(Type type, Input input)
        {
            var DataConnection = new DataConnection(LinqToDB.ProviderName.SqlServer2012, "ConnectionString");
            var t = typeof(DataConnection);
            var methodInfo = t.GetMethods().Where(m => m.Name == "GetTable" && m.GetParameters().Count() == 0).FirstOrDefault();
            var genericmethod = methodInfo.MakeGenericMethod(type);
            var Table = (ITable<T>)genericmethod.Invoke(DataConnection, null);

            var primaryKeyProperty = type.GetProperty(input.condition.primaryKey);
            var attr = primaryKeyProperty.GetCustomAttributes();
            var primarykryattrType = typeof(PrimaryKeyAttribute);
            var primaryKeyattr = primaryKeyProperty.GetCustomAttribute(primarykryattrType);
            if (primaryKeyProperty == null)
            {
                throw new Exception("Invalid identifier name.");
            }
            var whereExp = PropertyEquals<T>(primaryKeyProperty, input.condition.primaryKeyVal, primaryKeyProperty.PropertyType);

            //var data = Table.Where(exp).FirstOrDefault();
            var updateExp = BuildLambda(input.pairs);

            using (var trans = DataConnection.BeginTransaction(IsolationLevel.ReadUncommitted))
            {

                var transaction = Table.Where(whereExp).Update(updateExp);
                trans.Commit();
            }

        }
        public Expression<Func<T, T>> BuildLambda(Dictionary<string,string> columns)
        {
            var createdType = typeof(T);
            var displayValueParam = Expression.Parameter(typeof(T), "type");
            var ctor = Expression.New(createdType);
            List<MemberAssignment> valueAssingments = new List<MemberAssignment>();
            foreach (var column in columns)
            {
                var displayValueProperty = createdType.GetProperty(column.Key);
                var displayValueAssignment = Expression.Bind(
                    displayValueProperty, Expression.Constant(Convert.ChangeType(column.Value, displayValueProperty.PropertyType)));
                valueAssingments.Add(displayValueAssignment);
            }
            var memberInit = Expression.MemberInit(ctor, valueAssingments);
            return
                Expression.Lambda<Func<T, T>>(memberInit, displayValueParam);
        }
        public Expression<Func<T, bool>> PropertyEquals<T>(PropertyInfo property, string value, Type valueType)
        {
            var param = Expression.Parameter(typeof(T));
            var body = Expression.Equal(Expression.Property(param, property),
                Expression.Constant(Convert.ChangeType(value, valueType)));
            return Expression.Lambda<Func<T, bool>>(body, param);
        }

    }

}

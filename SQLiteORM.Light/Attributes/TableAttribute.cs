using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteORM.Attributes
{
    //[AttributeUsage(AttributeTargets.Class)]
    //public class TableAttribute : Attribute
    //{
    //    public string Name { get; }

    //    public TableAttribute(string name)
    //    {
    //        Name = name;
    //    }
    //}
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TableAttribute : Attribute
    {
        public string Name { get; set; }
        public string Schema { get; set; }

        public TableAttribute() { }

        public TableAttribute(string name, string schema = null)
        {
            Name = name;
            Schema = schema;
        }
    }
}

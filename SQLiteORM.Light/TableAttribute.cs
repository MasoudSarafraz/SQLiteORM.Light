using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteORM.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string Name { get; }

        public TableAttribute(string name)
        {
            Name = name;
        }
    }
}

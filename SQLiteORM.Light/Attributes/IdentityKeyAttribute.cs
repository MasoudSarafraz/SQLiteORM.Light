using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteORM.Attributes
{

    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class IdentityKeyAttribute : Attribute
    {
    }
}

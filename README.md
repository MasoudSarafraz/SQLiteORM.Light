# SQLiteORM – Lightweight ORM for SQLite in C#

SQLiteORM is a zero-configuration, convention-based object–relational mapper built on top of **System.Data.SQLite**.  
It gives you strongly-typed LINQ-like queries, automatic schema migration, and bulk operations without ever writing raw SQL.

---

## 🚀 Key Features

| Feature | Description |
|---|---|
| **Auto-Schema** | Tables and missing columns are created automatically at start-up. |
| **LINQ-style Queries** | Use C# expressions (`x => x.Age > 18`) that are translated to SQL. |
| **Bulk Operations** | `InsertList`, `UpdateList`, `DeleteList`, `Upsert`. |
| **Index Managment** | Create all type of index for table by Attributes |
| **Transaction Safety** | All multi-row operations run inside transactions. |
| **No Config Needed** | Works out of the box with a local `Database.sqlite` file. |
| **Configurable** | One line can switch DB path or file name. |
| **Performance** | Aggressive caching of reflection data and WAL journal mode. |

---

## 📦 Quick Start

### 1. Define a Model

```csharp
using SQLiteORM.Attributes;

[Table("Users")]
public class User
{
    [IdentityKey]           // or name the property "Id"
    public int UserId       { get; set; }
    Index("IX_Users_Name", IsUnique = true)]
    public string Name      { get; set; }
    public int  Age         { get; set; }
    public DateTime Joined  { get; set; }
    [Ignore]                // computed or not persisted
    public string Display   => $"{Name} ({Age})";
}
```

### 2. Write / Read Data

```csharp
// Insert
var user = new User { Name = "Alice", Age = 30, Joined = DateTime.UtcNow };
int id = SQLiteManager.Insert(user);

// Query
var adults = SQLiteManager.Find<User>(u => u.Age >= 18);

// Update
user.Age++;
SQLiteManager.Update(user);

// Delete
SQLiteManager.Delete<User>(user.UserId);
```

### ⚙️ Configuration

```csharp
SQLiteManager.SetDefaultDatabasePath("MyApp.sqlite", @"C:\Data");


```
or via AppConfig.xml:
```xml
<DatabaseConfig>
  <DatabaseName>MyApp.sqlite</DatabaseName>
  <DatabasePath>C:\Data</DatabasePath>
</DatabaseConfig>
```

🏎️ Bulk Operations
```csharp
var users = GetLotsOfUsers();
SQLiteManager.InsertList(users);          // ~1 transaction
SQLiteManager.UpdateList(users);

var ids = users.Select(u => u.UserId).ToList();
SQLiteManager.DeleteList<User>(ids);
```
Index Managment
```csharp
//get all index for table
var indexes = SQLiteManager.GetIndexesForTable("Users");
//check index exists
bool indexExists = SQLiteManager.IndexExists("IX_Users_Email");
//remove index by name
SQLiteManager2.RemoveIndex("IX_Users_Email");
//remove all index for table
SQLiteManager.RemoveIndexesForTable("Users");
//remove index for property you definde before
SQLiteManager.RemoveIndexesForProperty<User>(u => u.Email);
```
🧹 Schema Evolution
Add a new property to your model.
Restart the application → the column is added automatically without data loss.
Remove a column explicitly:

```csharp
SQLiteManager.RemoveColumn("Users", "LegacyColumn");

```

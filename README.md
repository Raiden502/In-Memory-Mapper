# **In-Memory Mapper**

Creating a Database where Users can define models with various fields, reflecting the schema of the database. The database operates entirely in-memory, storing data as objects, with no integration with traditional SQL or NoSQL databases. Additionally i developed a Custom ORM's that allows users to query the database using Pythonic syntax and Supports full CRUD operations, providing essential database management features similar to both Sql and NoSql.

⚠️ **Note**: This project is under development and is not yet production-ready. It is primarily intended for learning and exploring ORM-based database abstractions and database designing

## **Features**
- **In-Memory Database**: Operates entirely in memory, independent of external SQL/NoSQL databases.
- **Custom ORM**: Enables queries with intuitive, Pythonic syntax.
- **CRUD Operations**: Comprehensive support for Create, Read, Update, and Delete actions.
- **Model-Based Schema**: Easily define models with fields that represent the database schema.
- **Joins, Subqueries**: Execute complex queries with built-in support for subqueries, joins
- **Operator Callbacks**: Built-in support for comparison and string operators.
- **Datatypes and constraints**: Supports python datatypes and constraints.

## **Future improvements and features**
- support for aggregate functions, group by, triggers
- support for database server and client sdk, schema versioning
- support for persistent storage , data encryption and security features
- support for multi threading/processing and performance improvements

## **Installation and Usage**

### **Setup**
1. Clone this repository:
```
   git clone https://github.com/Raiden502/In-Memory-Mapper
   cd In-Memory-Mapper
```

2. Virtual environment and Requirements:
``` 
    .\venv\Scripts\activate
    pip install -r requirements.txt
```
## Run
Use `Version 1_4` of the module to define the schema for your models and begin interacting with the in-memory database.
```
    cd version_1_4
    py run.py
```

## Change Log
**Version 0.1: Initial Design and Schema Testing** 
- Created the schema for user-defined models.
- Extracted and tested model properties.

**Version 0.5 & 0.6: Database Design, Migrations and Operators**
- Designed the core in-memory database structure.
- Added support for database migrations.
- Added support for Orm's.
- Introduced callbacks for comparison and string operators.

**Version 1: Query Syntax and Execution**
- Implemented method chaining for queries.
- Unified the data exchange class for query execution.
- Added support for joins and query execution order.

**Version 1.1 & 1.3: Enhanced Query Features**
- Added built-in support for comparison and string operators.
- Simplified the model properties extraction process.
- Enabled subqueries for more complex queries.
- Optimized ORM syntax to align with traditional ORM designs.
- Resolved existing bugs and improved performance.

**Version 1.4: Data Types and CRUD Operations**
- Added built-in support for python data types
- Added constraints such as unique, nullable, default primary key.
- Added update and delete queries.

**Version 1.5: Alias, Common Table expression and expressions**
- Added support for alias for custom values,
- Added support for arithmetic operators, simple and complex expression calculation.
- Added support query from previous query and common table expression

## Contributing
Contributions are welcome! If you'd like to enhance the project or report issues, feel free to fork the repository, create a new branch, and submit a pull request.


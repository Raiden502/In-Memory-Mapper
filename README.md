# **In-Memory Mapper**

Designed and implemented a In-Memory Database, Users can define models with various fields, reflecting the schema of the database. The database operates entirely in-memory, storing data as objects, with no integration with traditional SQL or NoSQL databases. Additionally i developed a Custom ORM's that allows users to query the database using Pythonic syntax and Supports full CRUD operations, providing essential database management features. This Solution which easily integrates with existing server-side applications, offering a lightweight for in-memory data management.

⚠️ **Note**: This project is under development and is not yet production-ready. It is primarily intended for learning and exploring ORM-based database abstractions and database designing


## **Features**
- **In-Memory Database**: Operates entirely in memory, independent of external SQL/NoSQL databases.
- **Custom ORM**: Enables queries with intuitive, Pythonic syntax.
- **CRUD Operations**: Comprehensive support for Create, Read, Update, and Delete actions.
- **Model-Based Schema**: Easily define models with fields that represent the database schema.
- **Joins and Subqueries**: Execute complex queries with built-in support for subqueries and joins.
- **Operator Callbacks**: Built-in support for comparison and string operators.

## **Installation and Usage**

### **Setup**
1. Clone this repository:
   ```bash
   git clone https://github.com/Raiden502/In-Memory-Mapper
   cd In-Memory-Mapper
2. Activate the virtual environment:
    ``` 
    .\venv\Scripts\activate

3. Install the required packages:
    ```
    pip install -r requirements.txt


## Run
Use `Version 6` of the module to define the schema for your models and begin interacting with the in-memory database.

## Change Log
**Version 01 : Initial Design and Schema Testing** 
- Created the schema for user-defined models.
- Extracted and tested model properties.

**Version 02: Database Design and Migrations**
- Designed the core in-memory database structure.
- Added support for database migrations.
- Added support for Orm's.

**Version 03: Operator Support**
- Introduced callbacks for comparison and string operators.
- Improved support for Orm's.

**Version 04: Query Syntax and Execution**
- Implemented method chaining for queries.
- Unified the data exchange class for query execution.
- Added support for joins and query execution order.

**Version 05 & 06: Enhanced Query Features**
- Added built-in support for comparison and string operators.
- Simplified the model properties extraction process.
- Enabled subqueries for more complex queries.
- Optimized ORM syntax to align with traditional ORM designs.
- Resolved existing bugs and improved performance.

## Contributing
Contributions are welcome! If you'd like to enhance the project or report issues, feel free to fork the repository, create a new branch, and submit a pull request.


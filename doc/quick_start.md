![openGauss Logo](openGauss-logo.png "openGauss logo")

简体中文

- [图数据的DDL语句](#图数据的ddl语句)
  - [GRAPH](#graph)
  - [设置和显示当前图空间](#设置和显示当前图空间)
  - [LABEL](#label)
  - [属性图索引的创建](#属性图索引的创建)
- [图数据的DML语句](#图数据的dml语句)
  - [Cypher语句的使用](#cypher语句的使用)
    - [1. CREATE语句：点边数据的插入](#1-create语句点边数据的插入)
    - [2. MATCH语句：查询点边及路径](#2-match语句查询点边及路径)
    - [3. SET语句：修改点或边的属性](#3-set语句修改点或边的属性)
    - [4. DELETE语句: 删除指定的点或边](#4-delete语句-删除指定的点或边)
  - [SPARQL语句的使用](#sparql语句的使用)
    - [1. SPARQL路径查询](#1-sparql路径查询)
    - [2. SPARQL模式匹配查询](#2-sparql模式匹配查询)
- [数据导入](#数据导入)
  - [导入准备](#导入准备)
  - [属性图数据导入](#属性图数据导入)
  - [RDF图数据导入](#rdf图数据导入)
- [JDBC 使用示例](#jdbc-使用示例)
- [python 使用示例](#python-使用示例)

## 图数据的DDL语句
### GRAPH
1. 创建图空间
```
  # 创建名称为graphname的图空间，graphname是一个变量名，可替换为其他用户指定名称
  CREATE GRAPH graphname;
```
2. 删除图空间
```
  # 删除名称为graphname的图空间
  DROP GRAPH graphname CASCADE;
```
### 设置和显示当前图空间
1. 设置当前图空间
```
  SET graph_path = graphname; 
```
2. 显示当前的图空间
```
  SHOW graph_path; 
```
### LABEL
1. 创建VLABEL
```
  CREATE VLABEL person;
  CREATE VLABEL friend inherts (person);
```
2. 创建ELABEL
```
  CREATE ELABEL knows;
  CREATE ELABEL live_together;
  CREATE ELABEL roomate inherits(knows,live_together)
```
3. VLABEL 和 ELABEL 的删除
```
  DROP VLABEL friend;  # 删除 VLABEL
  DROP ELABEL roomate;   # 删除 ELABEL
```
### 属性图索引的创建
```
  # 使用属性id为label是person的数据创建唯一索引
  CREATE UNIQUE PROPERTY INDEX ON person(id)
```
## 图数据的DML语句 
openGauss-graph 同时支持Cypher语言和SPARQL语言。使用 CYPHER 和 SPARQL 两个关键字标记使用的查询语法。
具体Cypher 语法可参考：[openCypher](http://opencypher.org/resources/) , [SPARQL](https://www.w3.org/TR/sparql11-query/) .下边提供部分语法示例。
### Cypher语句的使用
#### 1. CREATE语句：点边数据的插入
```
  # 创建一个person节点，该节点属性name的值为 Tom 
  CYPHER CREATE (:person {id: 1 , name: 'Tom',age:20});

  # 创建一个person节点，该节点属性name的值为 Jack
  # 如果person的属性id创建了唯一索引，相同id将会返回错误提示
  CYPHER CREATE (:person {id: 2 , name: 'Jack',age:21});

  # 创建一个person节点，该节点属性name的值为 Mike
  CYPHER CREATE (:person {id: 3 , name: 'Mike',age:22});

  # 新建边
  CYPHER MATCH (p1:person {name: 'Tom'}) ,(p2:person {name: 'Jack'}) 
  CREATE (p1)-[:knows {fromdate:'2022-11-20'}]->(p2);

  CYPHER MATCH (p1:person {name: 'Jack'}) ,(p2:person {name: 'Mike'}) 
  CREATE (p1)-[:knows {fromdate:'2022-11-20'}]->(p2);

  # 可通过以下语句同时创建点和边
  CYPHER CREATE (:person {name: 'Pat',age:23})-[:knows {fromdate:'2022-11-21'}]->(:person {name: 'Nikki',age:24});
```
#### 2. MATCH语句：查询点边及路径
```
  # 查询全部点
  CYPHER MATCH (n)  RETURN n; 
  CYPHER MATCH (n)  RETURN n.name;

  # 查询边
  CYPHER MATCH (n)-[r]->(m)  RETURN r;
  CYPHER MATCH (n)-[r]->(m)  RETURN n,r,m;
  CYPHER MATCH (n)-[r]->(m)  RETURN r.fromdate;

  # 查询路径
  CYPHER MATCH p=(n)-[r]->(m)  RETURN p;
  CYPHER MATCH p=()-[]->()-[]->()  RETURN p;

  # where 子句
  CYPHER MATCH (n)  WHERE n.age > 22  RETURN n; 
  CYPHER MATCH (n)  WHERE n.name = 'Mike'  RETURN n;

  # order by 子句
  CYPHER MATCH (n)  RETURN n order by n.age; 
  CYPHER MATCH (n)  RETURN n order by n.age DESC;

  # skip ,limit 子句
  CYPHER MATCH (n)  RETURN n LIMIT 2; 
  CYPHER MATCH (n)  RETURN n ORDER BY n.age SKIP 1 LIMIT 2;


```

#### 3. SET语句：修改点或边的属性
```
# 修改匹配到的关系，SET子句既可以增加新的属性值，也可以修改旧值
  CYPHER MATCH (:person {name: 'Tom'})-[r:knows]->(:person {name: 'Jack'})
  SET r.since = '2022-11-11';

```
#### 4. DELETE语句: 删除指定的点或边
```
  # 删除匹配到的节点
  CYPHER MATCH (m:person {name: 'Tom'}) DELETE m;

  # 删除点时可能会提示 ERROR:  vertices with edges can not be removed 
  # 如果确定要删除的点，使用DETACH 关键字，同时删除该点关联的边
  CYPHER MATCH (m:person {name: 'Tom'}) DETACH DELETE m;

  # 删除匹配到的关系
  CYPHER MATCH (m:person {name: 'Jack'})-[l:knows]->(b:person {name: 'Tom'}) DELETE l;

```


### SPARQL语句的使用
#### 1. SPARQL路径查询
```
  SPARQL SELECT ?X, ?Y 
  WHERE {
      ?X <publicationAuthor> / <advisor> ?Y.
  };

```
#### 2. SPARQL模式匹配查询
```
  SPARQL SELECT ?X, ?Y, ?Z
  WHERE{
    ?X <memberOf> ?Z .
    ?Z <subOrganizationOf> ?Y .
    ?X <undergraduateDegreeFrom> ?Y .
  };

```


## 数据导入

### 导入准备

1. 安装 file_fdw 拓展
```
CREATE EXTENSION file_fdw;
```
> openGauss-graph 安装时自动安装该拓展。如果无法创建，进入源码包 contrib/file_fdw 目录执行 make && make install 手动安装
1. 创建数据导入服务
```
# import_server 是自定义的服务名称，后边导入数据需要这个服务
CREATE SERVER import_server FOREIGN DATA WRAPPER file_fdw;
```

### 属性图数据导入

> 这里以 ldbc 的comment数据导入示例
>
```
# 1. 第一步、创建外表

create foreign table fdwComment  # fdwComment 为创建外表的表名
(
   id int8,
   creationDate int8,
   locationIP varchar(80),
   browserUsed varchar(80),
   content varchar(2000),
   length int4
)
server import_server
options
(
 FORMAT 'csv',
 HEADER 'true',     # 是否包含标题头
 DELIMITER '|',
 NULL '',
 FILENAME '/path/to/comment.csv'
 );

# 2.  第二步、执行导入

CYPHER LOAD FROM fdwComment AS row 
CREATE (:comment = JSONB_STRIP_NULLS(
  TO_JSONB(ROW_TO_JSON(row))
));
```

### RDF图数据导入
> 这里以lubm数据集为例
```
# 1. 第一步、创建外表

create foreign table lubm_profile_regress    # lubm_profile_regress 为创建外表的表名
(
    subject Text,
    predicate Text,
    object Text,
    dot    char
)
server import_server
options
(
 FORMAT 'csv',                # 文件格式为CSV  
 HEADER 'false',              # 是否包含标题头
 DELIMITER ' ',             
 NULL '',                     
 FILENAME 'path/lubm1.nt'    # 导入文件的位置
 );

# 2. 第二步、执行导入

SPARQL LOAD lubm_profile_regress;

```

## JDBC 使用示例
1. jdbc 驱动下载
```
<!-- https://mvnrepository.com/artifact/org.opengauss/opengauss-jdbc -->
  <dependency>
      <groupId>org.opengauss</groupId>
      <artifactId>opengauss-jdbc</artifactId>
      <version>3.0.0</version>
  </dependency>
```
2. 获取连接
```java
 public static Connection getConnect(String username, String password) throws ClassNotFoundException, SQLException {
        // 驱动类
        String driver = "org.opengauss.Driver";
        //数据库连接描述符
        String jdbcURL = "jdbc:opengauss://127.0.0.1:5432/postgres";
        // 加载驱动
        Class.forName(driver);
        // 数据库用户名密码
        Properties info = new Properties();
        info.setProperty("user", username);
        info.setProperty("password", password);

        //创建连接
        Connection conn = DriverManager.getConnection(sourceURL, info);
        System.out.println("Connection succeed!");
        return conn;
 }
```
3. 执行图数据操作
```java
public class OpenGaussGraphQuery {
      public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // 获取连接
        Connection con = ConnectOpenGauss.getConnect("dblab", "dblab123_");
        Statement stmt = con.createStatement();
        // stmt.execute("DROP GRAPH test CASCADE;");
        // 创建图空间
        stmt.execute("CREATE GRAPH test;");
        // 设置当前图空间
        stmt.execute("SET graph_path = test;");
        // 插入点边数据
        stmt.execute("cypher CREATE (:person {name: 'Tom'})-[:knows {fromdate:'2022-11.21'}]->(:person {name: 'Summer'});");
        // 执行查询 这里使用cypher 示例。也可执行SPARQL查询
        ResultSet rs = stmt.executeQuery
                ("cypher MATCH (n)-[r]->(m) RETURN n,r,m");
        // 获取返回值头信息
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String catalogName = rsmd.getColumnName(i + 1);
            System.out.print(catalogName+"\t");
        }
        System.out.println();
        // 结果集遍历
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                // 这里实际返回一个PGobject。 可通过分析对象信息进一步细化开发
                Object object = rs.getObject(i + 1);
                System.out.print(object + "\t");
            }
            System.out.println();
        }
        // 连接释放
        con.close();
    }
}      
```
   
## python 使用示例 
1. 下载python驱动
```
pip install psycopg2
```
2. 查询示例 
``` python

import psycopg2 

# 获取连接
conn = psycopg2.connect(database="postgres", user="omm", password="xxxx", host="127.0.0.1", port="5432")

cur = conn.cursor()
# 设置图空间
cur.execute("set graph_path=lubm")
# 也可执行cypher语言的查询
# cur.execute("cypher match (n)-[r]->(m) return n,r,m")
# 使用SPARQL语言查询
cur.execute("SPARQL SELECT ?x, ?y  WHERE {  ?x <publicationAuthor> / <advisor> ?y. }")

rows = cur.fetchall()

des = cur.description
print("结果集头信息:", des)
# 获取结果集头信息
print("表头:", ",".join([item[0] for item in des]))
# 遍历结果集
for row in rows:
   print("x = ", row[0])
   print("y = ", row[1])
# 连接释放
conn.close()
```


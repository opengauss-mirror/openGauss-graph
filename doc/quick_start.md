# openGauss-graph使用文档

[TOC]

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

## 查询

### SPARQL路径查询处理

`路径查询`是SPARQL中的一种高级查询类型，通常用于查找两个或多个节点之间的路径。路径查询可以用于检索符合特定模式的三元组，例如检索所有河流连接的城市列表或检索所有城市到到达机场所需的旅游路线等。

#### 语法

SPARQL语句使用三元组来约束查询路径，三元组放在`WHERE`关键词后的花括号（{}）中。

SPARQL语句的基本语法结构如下：

```SPARQL
SPARQL SELECT <VALUES> WHERE {<TUPLES>};
```

`VALUES`：查询要输出的变量，多个变量间用逗号分隔。

`TUPLE`：三元组约束，多个约束间用点号分隔。

#### 三元组约束的类型

三元组类型的约束分为三种：类型约束、属性约束和路径约束。

1. 类型约束

类型约束是指限制查询结果只包含指定类型的资源或实例。SPARQL支持使用rdf:type 属性来过滤类型。可以通过如下的三元组形式来约束节点的类型：

```SPARQL
?X rdf:type <TYPE> .
```

`TYPE`：表示节点的类型

例如：

```SPARQL
?X rdf:type <Person>
```

表示返回类型是Person的节点。

2. 属性约束

类型约束是指限制查询结果中的对应节点需要包含某些特定属性值。可以通过如下的三元组形式来约束节点或边类型：

```SPARQL
?X <PROPERTY> <V>
```

`PROPERTY`：节点或边的属性名

`V`：节点或边的属性值

例如：

```SPARQL
?X <uri> "http://www.Department0.University0.edu/FullProfessor1/Publication10" .
```

这表示uri属性的属性值为http://www.Department0.University0.edu/FullProfessor1/Publication10的节点

3. 路径约束

路径约束可以用来描述节点之间的复杂关系。

最基本的路径约束是单跳路径。即两个节点间只需要经过一条边即可到达。

可以通过如下的三元组形式来表示：

```SPARQL
?X <RELATION> ?Y
```

`RELATION`：表示两个节点之间的关系，也就是两个节点直接的边。

例如：

```SPARQL
?X <teacherof> ?Y
```

这表示具有teacherof关系的所有节点对。

路径约束也支持多跳路径。表示经过连续的几条边所能到达的节点的路径。边类型之间用`/`相连。可以通过如下的三元组形式来表示：

```SPARQL
?X <R1> / <R2> / <R3> ?Y
```

`<R1> / <R2> / <R3>`：表示要路径要连续经过R1、R2、R3。

例如：

```SPARQL
?X <publicationAuthor> / <advisor> / <teacherof> ?Y.
```

表示经过?X、?Y只见存在一条经过publicationAuthor边，再经过advisor边，再经过teacherof的路径。

#### 路径查询示例

```SPARQL
SPARQL SELECT ?X, ?Y   WHERE {
  	?X rdf:type <Publication> .
  	?X <uri> "http://www.Department0.University0.edu/FullProfessor1/Publication10" .
    	?X <publicationAuthor> / <advisor> / <teacherof> ?Y.
};
```

### SPARQL星型查询

`星型查询`是指在RDF图中通过一组给定的主语，查询它们的共同宾语。在这种查询中，被查询的主语实体通常构成了一个星型模式，因此被称为星型查询。

#### 语法

`星型查询`也是使用三元组来进行约束。不过，不同于路径查询，星型查询是从一个中心节点寻找他的具有多种不同关系的邻居节点。星星查询语法如下：

```SPARQL
WHERE{
    ?X <R1> ?Y .
    ?X <R2> ?Z .
    ?X <Rn> ?E .
}
```

`R`：表示节点间的关系

`?Y ?Z ?E`：表示与中心节点?X的相邻节点

#### 星型查询示例

```SPARQL
SPARQL SELECT ?X, ?Y, ?Z, ?E
WHERE {
  ?X rdf:type <AssistantProfessor> .
  ?Y rdf:type <University> .
  ?Z rdf:type <Department> .
  ?E rdf:type <Course> .
  ?X <doctoralDegreeFrom> ?Y .
  ?X <worksFor> ?Z .
  ?X <teacherOf> ?E .
};
```

### SPARQL复杂查询

`复杂查询`是指在RDF图中进行比星型查询更复杂的模式匹配。与星型查询不同，复杂查询可以涉及多个主语、多个谓语和过滤条件等。复杂查询还支持正则路径查询。

#### 语法

复杂查询涉及多个主语、谓语，还要同时支持星型查询，多跳路径查询以及正则路径查询。因为复杂查询是众多类型查询的一个集合，没有特定的语法格式。

`星型查询，多跳路径查询`前面已经介绍了。

`正则路径查询`是一种在RDF图中进行路径匹配的查询方式。与传统的基于结构模式匹配的查询语言不同，SPARQL正则路径查询可以根据正则表达式匹配RDF图中的路径，以实现更加灵活的查询功能。

例如：

```SPARQL
?X <suborganizationof>{1,3} ?Y.
```

表示?X和?Y之间的路径上可能有1-3条边，而边的类型全部是suborganizationof。

#### 复杂查询示例

```SPARQL
SPARQL SELECT ?X, ?Y, ?Z, ?E
WHERE {
  ?X rdf:type <ResearchGroup> .
  ?Z rdf:type <fullProfessor> .
  ?E rdf:type <Course> .
  ?X <suborganizationof>{1,3} ?Y.
  ?X <uri> "http://.../ResearchGroup9".
  ?Z <worksfor> ?Y .
  ?Z <teacherOf> ?E .
};
```

示例当中既有多主语查询

```SPARQL
?Z <worksfor> ?Y 
?X <suborganizationof>{1,3} ?Y.
```

也有星型查询

```SPARQL
?Z <worksfor> ?Y .
?Z <teacherOf> ?E .
```

以及正则查询

```SPARQL
?X <uri> "http://.../ResearchGroup9".
```


###  支持LUBM提出的14个benchmark查询

LUBM（Lehigh University Benchmark）是一个基于语义Web的测试基准，旨在测试RDF图数据库的性能。14个SPARQL查询是在LUBM基准下定义的一组查询，旨在测试Graph Database中的查询操作。这些查询可以帮助我们评估Graph Database的性能。

14个benchmark查询的具体语句如下：

**Q1**

```SPARQL
SPARQL SELECT ?X WHERE
{
  ?X rdf:type <GraduateStudent> .
  ?X <takesCourse> <http://www.Department0.University0.edu/GraduateCourse0> .
};
```



**Q2**

```SPARQL
SPARQL SELECT ?X, ?Y, ?Z
WHERE
{
  ?X rdf:type <GraduateStudent> .
  ?Y rdf:type <University> .
  ?Z rdf:type <Department> .
  ?X <memberOf> ?Z .
  ?Z <subOrganizationOf> ?Y .
  ?X <undergraduateDegreeFrom> ?Y .
};
```



**Q3**

```SPARQL
SPARQL SELECT ?X WHERE
{
  ?X rdf:type <Publication> .
  ?X <publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0> .
};
```



**Q4**

```SPARQL
SPARQL SELECT ?X, ?Y1, ?Y2, ?Y3
WHERE{
  ?X rdf:type <FullProfessor> .
  ?X <worksFor> <http://www.Department0.University0.edu> .
  ?X <name> ?Y1 .
  ?X <emailAddress> ?Y2 .
  ?X <telephone> ?Y3 .
};
```



**Q5**

```SPARQL
SPARQL SELECT ?X WHERE
{
  ?X rdf:type <graduatestudent> .
  ?X <memberOf> <http://www.Department0.University0.edu> .
};
```



**Q6**

```SPARQL
SPARQL SELECT ?X WHERE {
  ?X rdf:type <graduatestudent>.
};
```



**Q7**

```SPARQL
SPARQL SELECT ?X, ?Y WHERE {
  ?X rdf:type <graduatestudent> .
  ?Y rdf:type <graduatecourse> .
  ?X <takesCourse> ?Y .
  <http://www.Department0.University0.edu/AssociateProfessor0> <teacherOf> ?Y.
};
```



**Q8**

```SPARQL
SPARQL SELECT ?X, ?Y, ?Z
WHERE {
  ?X rdf:type <graduatestudent> .
  ?Y rdf:type <Department> .
  ?X <memberOf> ?Y .
  ?Y <subOrganizationOf> <http://www.University0.edu> .
  ?X <emailAddress> ?Z.
};
```



**Q9**

```SPARQL
SPARQL SELECT ?X, ?Y, ?Z WHERE {
  ?X rdf:type <graduatestudent> .
  ?Y rdf:type <fullprofessor> .
  ?Z rdf:type <Course> .
  ?X <advisor> ?Y .
  ?Y <teacherOf> ?Z .
  ?X <takesCourse> ?Z .
};
```



**Q10**

```SPARQL
SPARQL SELECT ?X
WHERE
{
  ?X rdf:type <graduatestudent> .
  ?X <takesCourse> <http://www.Department0.University0.edu/GraduateCourse0> .
};
```



**Q11**

```SPARQL
SPARQL SELECT ?X
WHERE
{
  ?X rdf:type <ResearchGroup> .
  ?X <subOrganizationOf> <http://www.University0.edu> .
};
```



**Q12**

```SPARQL
SPARQL SELECT ?X, ?Y
WHERE{
  ?X rdf:type <FullProfessor> .
  ?Y rdf:type <Department> .
  ?X <worksFor> ?Y .
  ?Y <subOrganizationOf> <http://www.University0.edu> .
};
```



**Q13**

```SPARQL
SPARQL SELECT ?X WHERE
{
  ?X rdf:type <Person> .
  <http://www.University0.edu> <hasAlumnus> ?X .
};
```

**Q14**

```SPARQL
SPARQL SELECT ?X WHERE
{
  ?X rdf:type <Person> .
  <http://www.University0.edu> <hasAlumnus> ?X .
};
```

### Cypher路径查询

在Cypher中，路径是指由节点和边组成的序列，它们之间有特定的关系。`路径查询`允许从图形数据库中检索基于特定模式的数据。使用Cypher路径查询，可以指定一个开始节点和一个或多个结束节点，然后沿着一条或多条边遍历到达结束节点。

#### 语法

Cypher使用`MATCH`语句指定路径的模式。在进行查询时，`MATCH`语句使用的点可以重复出现，但边不能重复。

`MATCH`语法的基本结构如下：

```
MATCH <pattern>
```

`pattern`：`MATCH`语句支持匹配一个或多个模式，多个模式之间用英文逗号（,）分隔。例如`(a)-[]->(b),(c)-[]->(d)`。单个模式也可以包含多余两个节点，例如`(a)-[]->(b)-[]->(c)`。

#### 路径查询示例

```CYPHER
CYPHER MATCH (X:Publication{uri:'http://www.Department0.University0.edu/FullProfessor1/Publication10'})-[:publicationAuthor]->()-[:advisor]->()-[:teacherof]->(Y)
RETURN X.uri,Y.uri;
```

### Cypher星型查询

Cypher`星型查询`是一种常用的图查询模式，它可以用于查找以一个中心节点为中心的多个节点和关系。这种查询模式通常应用于许多实际场景中，如社交网络分析、知识图谱、推荐系统等。

#### 语法

类似于sparql星型查询，可以通过多个模式（每个模式都是一个三元组）组合的方式指定星型查询。也可以使用多于一个节点的单个模式来指定星型查询，或是将二者组合起来。

例如：

```CYPHER
CYPHER MATCH (a)<-[]-(b)-[]->(c);
```

或者

```CYPHER
CYPHER MATCH
(b)-[]->(a),
(b)-[]->(c);
```

或者

```CYPHER
CYPHER MATCH 
(a)<-[]-(b)-[]->(c),
(b)-[]->(e);
```



#### 星型查询示例

```CYPHER
CYPHER MATCH
	(X:AssistantProfessor)-[:doctoralDegreeFrom]->(Y:University),
	(X:AssistantProfessor)-[:worksFor]->(Z:Department),
	(X:AssistantProfessor)-[:teacherOf]->(E:Course)
return X.uri as X, Y.uri as Y, Z.uri as Z, E.uri as E;

```

### Cypher复杂查询

`复杂查询`是指在RDF图中进行比星型查询更复杂的模式匹配。与星型查询不同，复杂查询可以涉及多个主语、多个谓语和过滤条件等。复杂查询还支持正则路径查询。

#### 语法

复杂查询涉及多个主语、谓语，还要同时支持星型查询，多跳路径查询以及正则路径查询。因为复杂查询是众多类型查询的一个集合，没有特定的语法格式。并且各种查询类型在前面已经提到过了，对此不再赘述。

#### 复杂查询示例

```CYPHER
CYPHER MATCH 	(X:ResearchGroup{uri:‘http://www.Department0.University0.edu/ResearchGroup9’})-	[:suborganizationof*0..3]->(Y)<-[:worksfor]-(Z:FullProfessor),
	(Z:FullProfessor)-[:doctoralDegreeFrom]->(E:University)
return x.uri as x, y.uri as y, z.uri as z, e.uri as e;
```

### 支持 LDBC 7个短查询和12个长查询

LDBC（Linked Data Benchmark Council）cypher基准查询是一项基于Cypher查询语言的性能测试工具和标准，旨在评估图数据库系统在处理大规模知识图谱数据时的性能和可扩展性。

LDBC 7个短查询和12个长查询如下：

#### Short Query

**Q1**

```cypher
CYPHER MATCH (r:Person)-[:isLocatedInPerson]->(s:Place) WHERE r.id = 6597069767117
RETURN r.firstName AS firstName,   r.lastName AS lastName,   r.birthday AS birthday,   r.locationIp AS locationIP,   r.browserUsed AS browserUsed,   s.id AS placeId,   r.gender AS gender,   r.creationDate AS creationDate;
```



**Q2**

```SPARQL
CYPHER MATCH (person:Person)<-[:hasCreator]-(m:message)
WHERE person.id = 6597069767117
WITH m ORDER BY m.creationDate DESC
LIMIT 20
MATCH (m)-[:replyOf*0..]->(p:Post)-[:hasCreatorPost]->(c:Person) 
RETURN  m.id as messageId, COALESCE(m.content, m.imageFile),  m.creationDate AS messageCreationDate,   p.id AS originalPostId,   c.id AS originalPostAuthorId,   c.firstName as originalPostAuthorFirstName,   c.lastName as originalPostAuthorLastName 
ORDER BY messageCreationDate DESC LIMIT 20;
```

**Q3**

```CYPHER
CYPHER MATCH (person:Person)-[r:knows]->(friend:Person)
WHERE person.id = 6597069767117
RETURN   friend.id AS friendId,   friend.firstName AS firstName,   friend.lastName AS lastName,  r."creationDate" AS friendshipCreationDate  
ORDER BY friendshipCreationDate DESC, friendId ASC;
```



**Q4**

```cypher
CYPHER MATCH (m:Message) 
WHERE m.id = 3
RETURN  COALESCE(m.content, m.imageFile),   m.creationDate as creationDate;
```



**Q5**

```cypher
CYPHER MATCH (m:Message)-[:hasCreator]->(p:Person) 
WHERE m.id = 206158430586
RETURN   p.id AS personId,   p.firstName AS firstName,   p.lastName AS lastName;
```



**Q6**

```cypher
CYPHER MATCH (v1:Message)-[:replyOf*0..]->(v2:Post)<-[:containerOf]-(v3:Forum)-[:hasModerator]->(v4:Person) WHERE v1.id = 206158430586 RETURN v4.firstName, v4.lastName;
```



**Q7** 

```cypher
CYPHER MATCH (person:Person)<-[:hasCreator]-(message:Message)<-[l:likes]-(liker:Person)
WHERE person.id = 6597069767117
WITH DISTINCT id(liker) AS liker_id, liker, message, l."creationDate" AS likeTime, person ORDER BY liker_id, likeTime DESC
RETURN liker.Id AS personId, liker.FirstName, liker.LastName, likeTime, message.id AS messageId, COALESCE(message.content, message.imagefile) AS messageContent, (likeTime - message.creationdate) / (1000 * 60) AS latency, NOT exists((person)-[:knows]->(liker)) ORDER BY likeTime DESC, personId ASC
LIMIT 20;
```



#### Long Query

**Q1**

```cypher
CYPHER MATCH (p :Person) - [path:knows*1..3] ->(friend :Person { 'firstname': 'Carmen' })
WHERE
    p.id = 6597069767117 WITH friend,
    min(length(path)) AS distance
ORDER BY
    distance ASC,
    friend.lastName ASC,
    friend.id ASC
LIMIT 20 
MATCH (friend) - [:isLocatedInPerson] ->(friendCity :Place) 
OPTIONAL MATCH (friend) - [studyAt:studyAt] ->(uni :Organization) - [:isLocatedInOrgan] ->(uniCity :Place) 
WITH friend,
    collect(
        CASE
            uni.name is null
            WHEN true THEN 'null'
            ELSE [uni.name, studyAt."classYear", uniCity.name]
        END
    ) AS unis, friendCity, distance
    OPTIONAL MATCH (friend) - [worksAt:workAt] ->(company :Organization) - [:isLocatedInOrgan] ->(companyCountry :Place)
WITH friend,
    collect(
        CASE
            company.name is null
            WHEN true THEN 'null'
            ELSE [company.name, worksAt."workFrom", companyCountry.name]
        END
    ) AS companies,
    unis,
    friendCity,
    distance RETURN friend.id AS id,
    friend.lastName AS lastName,
    distance,
    unis,
    companies
ORDER BY
    distance ASC,
    lastName ASC,
    id ASC;
```

**Q2**

```cypher
CYPHER MATCH (p :Person) - [:knows] ->(friend :Person) < - [:hasCreator] -(message :Message)
WHERE
    p.id = 6597069767117
    AND message.creationDate <= 2308797243000 WITH friend,
    message
ORDER BY
    message.creationDate DESC,
    message.id ASC
LIMIT 20
RETURN friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    message.id AS messageId,
    COALESCE(message.content, message.imageFile),
    message.creationDate AS messageCreationDate;
```



**Q3**

```cypher
CYPHER MATCH (person :Person) - [:knows*1..2] ->(friend :Person)
WHERE
    person.id = 1129
    AND id(person) != id(friend) WITH DISTINCT friend MATCH (friend) < - [:hasCreator] -(messageX :Message) - [:isLocatedInMsg] ->(countryX :Place { name: 'Colombia' })
WHERE
    not exists (
        (friend) - [:isLocatedInPerson] ->() - [:isPartOf] ->(countryX)
    )
    AND messageX.creationDate >= 0
    AND messageX.creationDate < 2308797243000 WITH friend,
    count(DISTINCT messageX) AS xCount MATCH (friend) < - [:hasCreator] -(messageY :Message) - [:isLocatedInMsg] ->(countryY :Place { name: 'Colombia' })
WHERE
    not exists (
        (friend) - [:isLocatedInPerson] ->() - [:isPartOf] ->(countryY)
    )
    AND messageY.creationDate >= 0
    AND messageY.creationDate < 2308797243000 WITH friend,
    xCount,
    count(DISTINCT messageY) AS yCount RETURN friend.id AS friendId,
    friend.firstName AS friendFirstName,
    friend.lastName AS friendLastName,
    xCount,
    yCount,
    xCount + yCount AS xyCount
ORDER BY
    xyCount DESC,
    friendId ASC
LIMIT 20;
     

CYPHER MATCH (person :Person) - [:knows*1..2] ->(friend :Person)
WHERE
    person.id = 1129
    AND id(person) != id(friend) WITH DISTINCT friend MATCH (friend) <- [:hasCreator] -(messageX :Message) - [:isLocatedInMsg] ->(countryX :Place)
return countryX;
```



**Q4** 

```cypher
cypher MATCH (person:Person {id: 4398046511333 })-[:KNOWS]-(friend:Person), (friend)<-[:hasCreator]-(post:Post)-[:hasTag]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN 1275350400000 > post.creationDate and post.creationDate >= 1275350400000 THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN 1275350400000 > post.creationDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;
```



**Q5**

```cypher
cypher MATCH (person:Person)-[:knows*1..2]->(friend:Person)
WHERE person.id = 1129 AND id(person) != id(friend)
WITH DISTINCT friend
MATCH (friend)<-[membership:hasMember]-(forum:Forum)
WHERE membership."joinDate" > 1288612800000
OPTIONAL MATCH (friend)<-[:hasCreatorPost]-(post:Post)<-[:containerOf]-(forum)
WITH forum.id AS forumid, forum.title AS forumTitle, count(id(post)) AS postcount ORDER BY postCount DESC, forumid ASC
RETURN forumTitle, postCount
LIMIT 20;
```



**Q6**

```cypher
CYPHER MATCH (person:Person)-[:knows*1..2]->(friend:Person)
WHERE person.id = 1129 AND id(person) != id(friend)
WITH DISTINCT friend
MATCH (friend)<-[:hascreatorpost]-(friendPost:Post)
MATCH (friendPost)-[:hasTagPost]->(:Tag { name: 'J._R._R._Tolkien' })
MATCH (friendPost)-[:hasTagPost]->(commonTag:Tag)
WHERE commonTag.name <> 'Tiberius'
RETURN commonTag.name AS tagName, count( DISTINCT id(friendPost)) AS postCount ORDER BY postCount DESC, tagName ASC
LIMIT 20;
```



**Q7** 

```cypher
CYPHER MATCH (person:Person)<-[:hasCreator]-(message:Message)<-[l:likes]-(liker:Person)
WHERE person.id = 21990232556027
WITH DISTINCT id(liker) AS liker_id, liker, message, l."creationDate" AS likeTime, person
ORDER BY liker_id, likeTime DESC
RETURN liker.Id AS personId, liker.FirstName, liker.LastName, likeTime, message.id AS messageId, COALESCE(message.content, message.imagefile) AS messageContent, (likeTime - message.creationdate) / (1000 * 60) AS latency, NOT exists((person)-[:knows]->(liker)) ORDER BY likeTime DESC, personId ASC
LIMIT 20;

```



**Q8**

```cypher
CYPHER MATCH (start:Person {id: 143})<-[:HASCREATOR]-(:Message)<-[:REPLYOF]-(comment:Comment)-[:HASCREATOR]->(person:Person)
RETURN
    person.id AS personId,
    person.firstName AS personFirstName,
    person.lastName AS personLastName,
    comment.creationDate AS commentCreationDate,
    comment.id AS commentId,
    comment.content AS commentContent
ORDER BY
    commentCreationDate DESC,
    commentId ASC
LIMIT 20;
```



**Q9**

```cypher
CYPHER MATCH (person:Person)-[:knows*1..2]->(friend:Person)
WHERE person.id = 1129
WITH DISTINCT friend
MATCH (friend)<-[:hasCreator]-(message:Message)
WHERE message.creationDate < 2281459481000
WITH friend, message ORDER BY message.creationDate DESC, message.id ASC
LIMIT 20
RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS messageId, COALESCE(message.content, message.imageFile), message.creationDate AS messageCreationDate;

```



**Q10**

```cypher
CYPHER MATCH (person:Person)-[:knows*2..2]->(friend:Person)
WHERE person.id = 1129 AND ((to_jsonb(date_part(right(left("varchar"('month'), "int4"(6)), "int4"(5)), to_timestamp(int8(friend.birthday / 1000)))) = '1984-02-18T00:00:00+00:00' AND to_jsonb(date_part(right(left("varchar"('month'), "int4"(6)), "int4"(5)), to_timestamp(int8(friend.birthday / 1000)))) >= 21) OR (to_jsonb(date_part(right(left("varchar"('month'), "int4"(6)), "int4"(5)), to_timestamp(int8(friend.birthday / 1000)))) = (5 % 12)+1 AND to_jsonb(date_part(right(left("varchar"('month'), "int4"(6)), "int4"(5)), to_timestamp(int8(friend.birthday / 1000)))) < 22)) AND id(friend) != id(person) AND NOT exists((friend)-[:knows]->(person))
WITH DISTINCT person, friend
MATCH (friend)-[:isLocatedInPerson]->(city:Place)
OPTIONAL MATCH (friend)<-[:hasCreatorPost]-(post:Post)
WITH person, friend, city.name AS personCityName, post,
CASE WHEN exists((post)-[:hasTagPost]->()<-[:hasInterest]-(person)) THEN 1 ELSE 0 END AS common
WITH friend, personCityName, count( DISTINCT id(post)) AS postCount, sum(common) AS commonPostCount
RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore, friend.gender AS personGender, personCityName ORDER BY commonInterestScore DESC, personId ASC
LIMIT 20;
```



**Q11**

```cypher
CYPHER MATCH (person:Person)-[:knows*1..2]->(friend:Person)
WHERE person.id = 1129 AND id(person) != id(friend)
WITH DISTINCT friend
MATCH (friend)-[worksAt:workAt]->(company:Organization)-[:isLocatedInOrgan]->(:Place { name: 'Afghanistan' })
WHERE worksAt."workFrom" < 2019
RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, company.name AS companyName, worksAt."workFrom" AS workFromYear ORDER BY workFromYear ASC, friendId ASC, companyName DESC
LIMIT 20;
```



**Q12**

```cypher
CYPHER MATCH (person:Person)-[:knows]->(friend:Person)
WHERE person.id = 1129
OPTIONAL MATCH (friend)<-[:hasCreatorComment]-(c:"comment")-[:replyOfPost]->()-[:hasTagPost]->(tag:Tag), (tag:Tag)-[:hasType]->(tagClass:TagClass)-[:isSubclassOf*0..]->(baseTagClass:TagClass)
WHERE tagClass.name = ? OR baseTagClass.name = ?
RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, collect( DISTINCT tag.name) AS tagNames, count( DISTINCT id(c)) AS count ORDER BY count DESC, friendId ASC
LIMIT 20;
```

### PageRank算子

`PageRank`算法是一种计算链接网络中网页重要性的算法，它可以衡量一个网页在整个互联网中的影响力和重要性。在图数据库中，PageRank 算法根据节点入度和相关节点的重要性来衡量图中每个节点的重要性。

#### 语法

```SPARQL
SPARQL ALGO PAGERANK('<NAME>');
```

`NAME`：表示图的名字

返回值为图中各个节点的PageRank值。

#### 算法

PageRank是一种迭代算法，其中前一次迭代的顶点的PageRank分数用于计算新的PageRank分数。顶点$v$在第$i$次迭代中的PageRank分数，由$PR(v_i)$表示为：
$$
PR(v_i)= {{(1−d)}/N}+{d∑_{u∈M(v)}(PR(u_{i−1})/L(u))}
$$
其中$N$是图中的顶点数，$d$是阻尼因子，$M(v)$表示指向顶点$v$的边的顶点集合$L(u)$表示顶点$u$的出度，$PR(u_{i−1})$表示第$i−1$次迭代中顶点$u$的PageRank分数。

#### PageRank算子示例

```SPARQL
SPARQL ALGO PAGERANK('connect');
```


### 最短路径算子

计算单源最短路(SSSP)，通过指定图中的源节点和目标节点，返回符合匹配条件的一条最短路径。

#### 语法

```SPARQL
SPARQL ALGO SHORTESTPATH('<NAME>''<S>''<D>');
```

`NAME`：表示图的名字

`S`：表示起始节点

`D`：表示目的节点

#### 最短路径算子示例

```SPARQL
SPARQL ALGOS SHORTESTPATH('test.connect' '3.1' '3.3');
```


### 广度优先遍历算子

指定图中的源节点，返回其他节点距离此节点的最小跳数

#### 语法

```SPARQL
SPARQL ALGO BREATHFS('<NAME>''<S>');
```

`NAME`：表示图的名字

`S`：表示起始节点

#### 广度优先遍历算子示例

```SPARQL
SPARQL ALGOS SHORTESTPATH('test.connect' '3.1' '3.3');
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


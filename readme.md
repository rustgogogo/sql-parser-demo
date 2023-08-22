# SQL解析

## 1. DQL

> MYSQL中将SELECT当作DML

### 1.1 SELECT

```sql
SELECT ID, NAME, AGE FROM DB1.TB1 as t1, TB2 as t2 WHERE AGE > 20 ORDER BY AGE DESC, ID ASC LIMIT 10 OFFSET 2;
```

## 2. DML

[13.2 Data Manipulation Statements](https://dev.mysql.com/doc/refman/8.0/en/sql-data-manipulation-statements.html)

### 2.1 INSERT

```sql
INSERT INTO a.TB1 (NAME,AGE,FLAG) VALUES('ZHANG_SAN', 20, true);
```

### 2.2 UPDATE

```sql
UPDATE TB1 SET NAME = 'name1', FLAG = false WHERE AGE > 10;
```

### 2.3 DELETE

## 3. DDL

[13.1 Data Definition Statements](https://dev.mysql.com/doc/refman/8.0/en/sql-data-definition-statements.html)

### 3.1 CREATE

### 3.2 ALTER

### 3.3 DROP

### 3.4 TRUNCATE
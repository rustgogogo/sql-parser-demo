use fmt::Display;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::ops::Deref;

use anyhow::{Error, Result};
use sqlparser::ast;
use sqlparser::ast::{Expr, OrderByExpr, Query, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::{Parser, ParserError};

const DIALECT: MySqlDialect = MySqlDialect {};

#[derive(Debug, Default)]
struct TableInfo {
    index: usize,
    schema: String,
    table: String,
    alias: String,
}

impl Display for TableInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.schema.len() == 0 {
            write!(f, "table{}: {}", self.index, self.table)
        } else {
            write!(f, "table{}: {}.{}", self.index, self.schema, self.table)
        }
    }
}

#[derive(Debug)]
enum MysqlValue {
    None,
    String(String),
    Number(u64),
    Boolean(bool),
}

#[derive(Debug, Default)]
struct SqlStruct {
    table_infos: Vec<TableInfo>,
    get_fields: Vec<String>,
    set_fields: HashMap<String, MysqlValue>,
    order_by_fields: HashMap<String, bool>,
    limit: Option<i32>,
    offset: Option<i32>,
    where_exist: bool,
}

impl Display for SqlStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut table_info_str = String::new();
        table_info_str.push_str("[");
        for table_info in &self.table_infos {
            table_info_str.push_str(&table_info.to_string());
            table_info_str.push_str(", ");
        }

        if table_info_str.len() > 1 {
            table_info_str.pop();
            table_info_str.pop();
        }

        table_info_str.push_str("]");

        let mut select_fields_str = String::new();
        select_fields_str.push_str("[");
        for select_field in &self.get_fields {
            select_fields_str.push_str(select_field);
            select_fields_str.push_str(", ");
        }

        if select_fields_str.len() > 1 {
            select_fields_str.pop();
            select_fields_str.pop();
        }

        select_fields_str.push_str("]");

        let mut set_fields_str = String::new();
        set_fields_str.push_str("[");
        for (k, v) in &self.set_fields {
            set_fields_str.push_str(k);
            set_fields_str.push_str("=");
            set_fields_str.push_str(&*format!("{:?}", v));
            set_fields_str.push_str(", ");
        }

        if set_fields_str.len() > 1 {
            set_fields_str.pop();
            set_fields_str.pop();
        }

        set_fields_str.push_str("]");

        let mut order_by_fields_str = String::new();
        for (order_by_field, order_by_value) in &self.order_by_fields {
            order_by_fields_str.push_str(order_by_field);
            order_by_fields_str.push_str(" ");
            if *order_by_value {
                order_by_fields_str.push_str("ASC");
            } else {
                order_by_fields_str.push_str("DESC");
            }
            order_by_fields_str.push_str(", ");
        }
        order_by_fields_str.pop();
        order_by_fields_str.pop();

        write!(
            f,
            "table_infos: {}, select_fields: {}, set_fields: {}, order_by_fields: {}, limit: {:?}, offset: {:?}, where_exist: {}",
            table_info_str, select_fields_str, set_fields_str, order_by_fields_str, self.limit, self.offset, self.where_exist
        )
    }
}

/*
解析SELECT语句
 */
fn parse_select() -> Result<SqlStruct, Error> {
    let sql = "SELECT ID, NAME, AGE FROM DB1.TB1 as t1, TB2 as t2 WHERE AGE > 20 ORDER BY AGE DESC, ID ASC LIMIT 10 OFFSET 2;";
    let select_parse_result: Result<Vec<Statement>, ParserError> = Parser::parse_sql(&DIALECT, sql);
    if select_parse_result.is_err() {
        let err_msg = select_parse_result.err().unwrap();
        return Err(Error::msg(err_msg));
    }

    let statements = select_parse_result.unwrap();
    let statement = &statements[0];

    let mut sql_struct = SqlStruct::default();

    if let Statement::Query(query) = statement {
        let Query {
            body,
            order_by,
            limit,
            offset,
            ..
        } = query.deref();

        if let SetExpr::Select(select) = body.deref() {
            let Select {
                projection: select_expr_vec,
                from: table_expr_vec,
                selection: where_expr,
                ..
            } = select.deref();

            for select_item in select_expr_vec {
                if let ast::SelectItem::UnnamedExpr(select_expr) = select_item {
                    if let Expr::Identifier(select_field_ident) = select_expr {
                        sql_struct.get_fields.push(select_field_ident.value.clone());
                    }
                } else if let ast::SelectItem::ExprWithAlias { expr: select_expr, .. } = select_item {
                    if let Expr::Identifier(select_field_ident) = select_expr {
                        sql_struct.get_fields.push(select_field_ident.value.clone());
                    }
                }
            }

            for (index, table_expr) in table_expr_vec.iter().enumerate() {
                let ast::TableWithJoins {
                    relation: table_factor,
                    ..
                } = table_expr.deref();

                if let TableFactor::Table {
                    name: object_name,
                    alias: alias_option,
                    ..
                } = table_factor {
                    let table_ident_vec = &object_name.0;
                    let table_ident_len = table_ident_vec.len();
                    let alias_name = {
                        if let Some(alias) = alias_option {
                            &alias.name.value
                        } else {
                            ""
                        }
                    };

                    let mut table_info = TableInfo::default();
                    table_info.index = index + 1;
                    table_info.alias = alias_name.to_string();
                    if table_ident_len == 2 {
                        table_info.schema = table_ident_vec[0].value.clone();
                        table_info.table = table_ident_vec[1].value.clone();
                        sql_struct.table_infos.push(table_info);
                    } else if table_ident_len == 1 {
                        table_info.table = table_ident_vec[0].value.clone();
                        sql_struct.table_infos.push(table_info);
                    }
                }
            }

            sql_struct.where_exist = where_expr.is_some();
        }

        if let Some(limit_expr) = limit {
            if let Expr::Value(limit_value) = limit_expr {
                if let ast::Value::Number(limit_number, ..) = limit_value {
                    sql_struct.limit = Some(limit_number.parse::<i32>().unwrap());
                }
            }
        }

        if let Some(offset_struct) = offset {
            let offset_value = &offset_struct.value;
            if let Expr::Value(offset_value) = offset_value {
                if let ast::Value::Number(offset_number, ..) = offset_value {
                    sql_struct.offset = Some(offset_number.parse::<i32>().unwrap());
                }
            }
        }

        let mut order_by_fields: HashMap<String, bool> = HashMap::with_capacity(order_by.len());
        for order_item in order_by {
            let OrderByExpr { expr: order_by_expr, asc, .. } = order_item;
            if let Expr::Identifier(order_by_field_ident) = order_by_expr {
                let order_value = asc.unwrap_or(false);
                order_by_fields.insert(order_by_field_ident.value.clone(), order_value);
            }
        }

        sql_struct.order_by_fields = order_by_fields;
    }

    Ok(sql_struct)
}

fn parse_insert() -> Result<SqlStruct, Error> {
    let sql = "INSERT INTO a.TB1 (NAME,AGE,FLAG) VALUES('ZHANG_SAN', 20, true);";
    let select_parse_result: Result<Vec<Statement>, ParserError> = Parser::parse_sql(&DIALECT, sql);
    if select_parse_result.is_err() {
        let err_msg = select_parse_result.err().unwrap();
        return Err(Error::msg(err_msg));
    }

    let statements = select_parse_result.unwrap();
    let statement = &statements[0];

    let mut sql_struct = SqlStruct::default();

    if let Statement::Insert { table_name: table_name_vec, columns, source: query, .. } = statement {
        let table_ident_vec = &table_name_vec.0;
        let table_ident_len = table_ident_vec.len();
        if table_ident_len == 2 {
            sql_struct.table_infos.push(TableInfo {
                schema: table_ident_vec[0].value.clone(),
                table: table_ident_vec[1].value.clone(),
                ..Default::default()
            });
        } else if table_ident_len == 1 {
            sql_struct.table_infos.push(TableInfo {
                table: table_ident_vec[0].value.clone(),
                ..Default::default()
            });
        }

        let mut insert_field_name_vec: Vec<String> = Vec::new();

        sql_struct.set_fields = HashMap::new();

        for column in columns {
            sql_struct.set_fields.insert(column.value.clone(), MysqlValue::None);
            insert_field_name_vec.push(column.value.clone());
        }

        let Query {
            body, ..
        } = query.deref();

        let mut insert_field_index = 0;

        match body.deref() {
            SetExpr::Values(values) => {
                let rows = &values.rows;
                for row in rows {
                    for item in row {
                        if let Expr::Value(value) = item {
                            if let ast::Value::Number(num_str, _) = value {
                                let field_name = insert_field_name_vec.get(insert_field_index).unwrap();
                                sql_struct.set_fields.insert(field_name.clone(), MysqlValue::Number(num_str.parse::<u64>().unwrap()));
                                insert_field_index = insert_field_index + 1;
                            } else if let ast::Value::SingleQuotedString(str_val) = value {
                                let field_name = insert_field_name_vec.get(insert_field_index).unwrap();
                                let s = str_val.clone();
                                sql_struct.set_fields.insert(field_name.clone(), MysqlValue::String(s));
                                insert_field_index = insert_field_index + 1;
                            } else if let ast::Value::Boolean(b) = value {
                                let field_name = insert_field_name_vec.get(insert_field_index).unwrap();
                                sql_struct.set_fields.insert(field_name.clone(), MysqlValue::Boolean(*b));
                                insert_field_index = insert_field_index + 1;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(sql_struct)
}

fn parse_update() -> Result<SqlStruct, Error> {
    let sql = "UPDATE TB1 SET NAME = 'name1', FLAG = false WHERE AGE > 10;";
    let update_parse_result: Result<Vec<Statement>, ParserError> = Parser::parse_sql(&DIALECT, sql);
    if update_parse_result.is_err() {
        let err_msg = update_parse_result.err().unwrap();
        return Err(Error::msg(err_msg));
    }

    let statements = update_parse_result.unwrap();
    let statement = &statements[0];

    let mut sql_struct = SqlStruct::default();
    if let Statement::Update {
        table,
        assignments,
        selection: where_expr,
        ..
    } = statement {
        let table_relation = table.clone().relation;
        if let TableFactor::Table { name: table_name_vec, .. } = table_relation {
            let table_ident_vec = &table_name_vec.0;
            let table_ident_len = table_ident_vec.len();
            if table_ident_len == 2 {
                sql_struct.table_infos.push(TableInfo {
                    schema: table_ident_vec[0].value.clone(),
                    table: table_ident_vec[1].value.clone(),
                    ..Default::default()
                });
            } else if table_ident_len == 1 {
                sql_struct.table_infos.push(TableInfo {
                    table: table_ident_vec[0].value.clone(),
                    ..Default::default()
                });
            }
        }

        sql_struct.set_fields = HashMap::new();

        for assignment in assignments {
            let key: &String = &assignment.id[0].value;
            let value = &assignment.value;

            if let Expr::Value(value) = value {
                if let ast::Value::Number(num_str, _) = value {
                    sql_struct.set_fields.insert(key.clone(), MysqlValue::Number(num_str.parse::<u64>().unwrap()));
                } else if let ast::Value::SingleQuotedString(str_val) = value {
                    let s = str_val.clone();
                    sql_struct.set_fields.insert(key.clone(), MysqlValue::String(s));
                } else if let ast::Value::Boolean(b) = value {
                    sql_struct.set_fields.insert(key.clone(), MysqlValue::Boolean(*b));
                }
            }
        }

        sql_struct.where_exist = where_expr.is_some();
    };

    Ok(sql_struct)
}

fn parse_delete() -> Result<SqlStruct, Error> {
    let sql = "DELETE FROM TB1 WHERE AGE > 10;";
    let select_parse_result: Result<Vec<Statement>, ParserError> = Parser::parse_sql(&DIALECT, sql);
    if select_parse_result.is_err() {
        let err_msg = select_parse_result.err().unwrap();
        return Err(Error::msg(err_msg));
    }

    let statements = select_parse_result.unwrap();
    let statement = &statements[0];

    let mut sql_struct = SqlStruct::default();

    if let Statement::Delete { from: table_expr_vec, selection: where_expr, .. } = statement {
        for (index, table_expr) in table_expr_vec.iter().enumerate() {
            let ast::TableWithJoins {
                relation: table_factor,
                ..
            } = table_expr.deref();

            if let TableFactor::Table {
                name: object_name,
                alias: alias_option,
                ..
            } = table_factor {
                let table_ident_vec = &object_name.0;
                let table_ident_len = table_ident_vec.len();
                let alias_name = {
                    if let Some(alias) = alias_option {
                        &alias.name.value
                    } else {
                        ""
                    }
                };

                let mut table_info = TableInfo::default();
                table_info.index = index + 1;
                table_info.alias = alias_name.to_string();
                if table_ident_len == 2 {
                    table_info.schema = table_ident_vec[0].value.clone();
                    table_info.table = table_ident_vec[1].value.clone();
                    sql_struct.table_infos.push(table_info);
                } else if table_ident_len == 1 {
                    table_info.table = table_ident_vec[0].value.clone();
                    sql_struct.table_infos.push(table_info);
                }
            }
        }

        sql_struct.where_exist = where_expr.is_some();
    }

    Ok(sql_struct)
}

fn parse_create() -> Result<SqlStruct, Error> {
    let sql = "CREATE TABLE TB1 (ID INT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(20) NOT NULL COMMENT '姓名', AGE INT, FLAG BOOLEAN);";
    let create_parse_result: Result<Vec<Statement>, ParserError> = Parser::parse_sql(&DIALECT, sql);
    if create_parse_result.is_err() {
        let err_msg = create_parse_result.err().unwrap();
        return Err(Error::msg(err_msg));
    }

    let statements = create_parse_result.unwrap();
    let statement = &statements[0];

    let mut sql_struct = SqlStruct::default();

    if let Statement::CreateTable { name: table_name_vec, columns, .. } = statement {
        let table_ident_vec = &table_name_vec.0;
        let table_ident_len = table_ident_vec.len();
        if table_ident_len == 2 {
            sql_struct.table_infos.push(TableInfo {
                schema: table_ident_vec[0].value.clone(),
                table: table_ident_vec[1].value.clone(),
                ..Default::default()
            });
        } else if table_ident_len == 1 {
            sql_struct.table_infos.push(TableInfo {
                table: table_ident_vec[0].value.clone(),
                ..Default::default()
            });
        }

        sql_struct.set_fields = HashMap::new();

        for column in columns {
            sql_struct.set_fields.insert(column.name.value.clone(), MysqlValue::None);
        }
    }

    Ok(sql_struct)
}

fn main() {
    let mut parser_func_map: HashMap<String, fn() -> Result<SqlStruct>> = HashMap::new();
    parser_func_map.insert("SELECT".to_string(), parse_select);
    parser_func_map.insert("INSERT".to_string(), parse_insert);
    parser_func_map.insert("UPDATE".to_string(), parse_update);
    parser_func_map.insert("DELETE".to_string(), parse_delete);
    parser_func_map.insert("CREATE".to_string(), parse_create);

    for (parser_type, parser_func) in parser_func_map {
        let parser_result = parser_func();
        if let Some(parser_struct) = parser_result.as_ref().ok() {
            println!("{}解析结果：{}\n", parser_type, parser_struct);
        } else {
            println!("{}解析失败：{:?}\n", parser_type, parser_result.err().unwrap());
        }
    }
}

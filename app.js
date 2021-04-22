const express = require('express');
const faker = require('faker');
const _ = require('lodash');
const mysql = require('mysql');
const fs = require('fs')
const converter = require('json-2-csv');
const  { parse } = require("fast-csv");
const { response } = require("express");
var bodyParser = require('body-parser');

var con = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "root",
    database: "simplifybase"
});

con.connect((err) => {
    if(err) throw err;
    console.log("connected");
});

const app = express();

app.use(bodyParser.json());

app.get('/insert', (request, response) => {
    console.log("executing insert");

    var sql = "CREATE TABLE IF NOT EXISTS customers(id INT, name VARCHAR(255), PRIMARY KEY(id))";
    			// "create table if not exists orders(oid INT, odate VARCHAR(255), cid INT, primary key(id), foreign key(cid) references customers(id));";
    con.query(sql, (err, res) => {
        if(err) throw err;
        console.log('inside create table customers query');

        if(res.warningCount == 0) {
        	// console.log('inside warning count');
            data = [];
            var cid = 0;

            data.push(
                _.times(1000000, () => {
                    const rname = faker.name.findName();
                    // if(cid<10) console.log(rname);
                    cid++;
                    return {
                        id: cid,
                        name: rname
                    };
                })
            );

            converter.json2csv(data[0], (err, csv) => {
                if(err) throw err;
                console.log('inside json2csv');
                fs.writeFileSync('customers.csv', csv);

                sql1 = `LOAD DATA LOCAL INFILE '/home/nimish/cp/webd/expressjs/simplify/customers.csv' INTO TABLE customers FIELDS TERMINATED BY "," LINES TERMINATED BY "\n" IGNORE 1 LINES`
                con.query(sql1, (err, res) => {
                    if(err) throw err;

                    console.log(res);
                    sql2 = `CREATE INDEX c_index ON customers(name)`;
                    con.query(sql2, (err, res) => {
                        if(err) throw err;

                        console.log(res);

                    });
                });
            });
        }
    });

    var sql = "CREATE TABLE IF NOT EXISTS orders(oid INT, date DATE, cid INT, PRIMARY KEY(oid), FOREIGN KEY(cid) REFERENCES customers(id))";
    			// "create table if not exists orders(oid INT, odate VARCHAR(255), cid INT, primary key(id), foreign key(cid) references customers(id));";
    con.query(sql, (err, res) => {
        if(err) throw err;
        console.log('inside create table query');

        if(res.warningCount == 0) {
        	console.log('inside warning count');
            data = [];
            var oid = 0;

            data.push(
                _.times(1000000, () => {
                    const cid = faker.datatype.number({min:1, max:1000000});
                    const d = faker.date.between('2000-01-01', '2021-04-22');
                    var str = d.getFullYear() + '-' + d.getMonth() + '-' + d.getDate();
                    console.log(str);
                    // const odate = faker.datatype.number({min:2020, max:2021}) + '-' + faker.datatype.number({min:1, max:12}) + '-' + faker.datatype.number({min:1, max:30});
                    // console.log(oid);
                    oid++;
                    return {
                        oid: oid,
                        date: str,
                        cid: cid
                    };
                })
            );

            converter.json2csv(data[0], (err, csv) => {
                if(err) throw err;
                console.log('inside json2csv');
                fs.writeFileSync('orders.csv', csv);

                sql1 = `LOAD DATA LOCAL INFILE '/home/nimish/cp/webd/expressjs/simplify/orders.csv' INTO TABLE orders FIELDS TERMINATED BY "," LINES TERMINATED BY "\n" IGNORE 1 LINES`
                con.query(sql1, (err, res) => {
                    if(err) throw err;

                    console.log(res);
                    sql2 = `CREATE INDEX cid_index ON orders(cid, date)`;
                    con.query(sql2, (err, res) => {
                        if(err) throw err;

                        console.log(res);

                    });
                });
            });
        }
    });

    console.log('Insertion Complete');
    response.sendStatus(200);
});

app.get('/drop', (req, res) => {
    var sql = "DROP TABLE IF EXISTS temp";
    con.query(sql, (err, res) => {
        if(err) {
            throw err;
            return;
        }
        console.log('table dropped');
    });

    sql = "DROP TABLE IF EXISTS orders";
    con.query(sql, (err, res) => {
        if(err) {
            throw err;
            return;
        }
        console.log('table dropped');
    });

    sql = "DROP TABLE IF EXISTS customers";
    con.query(sql, (err, res) => {
        if(err) {
            throw err;
            return;
        }
        console.log('table dropped');
    });
});

app.get('/search/:id', (req, response) => {
    var sql = "";
    if(req.params.id == 1) {
        sql = "SELECT count(*) from customers where id in (SELECT cid from orders group by(cid) having count(*) > 5)";
    }
    else if(req.params.id == 2) {
        sql = "SELECT id, name from customers where id in (SELECT cid from orders where date < date_sub(curdate(), INTERVAL 30 DAY)) and name like 'Dal%'";
    }
    else if(req.params.id==3) {
        sql = "SELECT name FROM customers WHERE id IN (SELECT cid FROM orders WHERE date < DATE_SUB(CURDATE(), INTERVAL 30 DAY) GROUP BY(cid) HAVING COUNT(*) > 5)";
    }
    else {
        res.send("wrong query");
        return;
    }
    con.query(sql, (err, res) => {
        if(err) {
            throw err;
            return;
        }
        response.json(res);
    });
});

app.listen(3000, () => {
    console.log("server is running on port 3000...");
});
const express = require('express');
const faker = require('faker');
const _ = require('lodash');
const mysql = require('mysql');
const fs = require('fs')
const converter = require('json-2-csv');
const  { parse } = require("fast-csv");
const { response } = require("express");

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

app.get('/insert', (request, response) => {
    console.log("executing insert");

    const d = faker.date.between('2015-01-01', '2021-04-22');
    var str = d.getFullYear() + '-' + d.getMonth() + '-' + d.getDate();
    console.log(str);
    console.log('Insertion Complete');
    response.sendStatus(200);
});

app.listen(4000, () => {
    console.log("server is running on port 3000...");
});
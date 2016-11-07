const fs = require('fs');
const Sequelize = require('sequelize');
const Promise = require('bluebird');
const http = require('request');
const _ = require('lodash');
Promise.promisifyAll(http);
const nconf = require('nconf');
nconf.argv()
 // .env() 
 //.file('config', { file: 'config/' + process.env.NODE_ENV + '.json' })
  .file('config', { file: __dirname + "/config/" + 'prod' + '.json' }); 
  
  var connectionInfo = {
  connectionString: nconf.get('connectionString')
    };
console.log("Starting sequelize " + connectionInfo.connectionString);
const sequelize = new Sequelize(connectionInfo.connectionString, {
    dialect: 'postgres',
    logging: false,
    pool: {
        max: 5,
        min: 0,
        idle: 10000
    },
    define: {
        timestamps: true,
        paranoid: true,
        underscoredAll: true,
        underscored: true
    }
});

const TOKEN = "postgres";

sequelize.query("truncate table follain.vend_raw_products",function(err,rows){
  if(err) throw exit(99);
  console.log('raw table truncated');
});

var sinceDate=new Date();
var sinceDateC=sinceDate.toISOString()
console.log('Date'+sinceDateC)
const productFiles = fs.readdirSync(__dirname + "/files/products").filter(f => f != ".DS_Store");
Promise.map(productFiles, loadProducts, {concurrency: 10});

function loadProducts(file){
    const data = JSON.parse(fs.readFileSync(__dirname + "/files/products/" + file, "UTF-8"));

    console.log(file + " : " + data.body.products.length + " records");

    return Promise.all(data.body.products.map(data => {
        return sequelize.query("INSERT INTO follain.vend_raw_products (id, data,asof_time) VALUES (:id,:data,:sinceDate)", {replacements: {id: [data.id], data: [JSON.stringify(data)],sinceDate: sinceDateC}})
            .catch( err => {
                console.log("got an error: ", err);

            })
    }));
}

console.log("Completed loading products");


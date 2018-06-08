const mongodb = require('mongodb');
const async = require('async');

const url = 'mongodb://localhost:27017';
const batchSize = parseInt(process.argv[2], 10) || 100;
const startTime = new Date();

console.log(`Migration started with batch size: ${batchSize}`);

mongodb.MongoClient.connect(url, (error, client) => {
  if (error) {
    console.error(error);
    return process.exit(1);
  }
  console.log('Connected to database');

  const db = client.db('migration');
  const dbName = 'customers';
  const customerData = Array.prototype.slice.call(require('./customer-data.json'));
  const customerAdddressData = Array.prototype.slice.call(require('./customer-address-data.json'));
  const rowCount = customerData.length;
  const queryCount = rowCount / batchSize;

  console.log(`Customer count ${rowCount}, will result in ${queryCount} queries`);

  // Delete any existing records
  db.collection(dbName).deleteMany({}, (error, result) => {
    if (error) {
      console.error(error);
      return process.exit(1);
    }
    // Prepare tasks to execute in parallel
    let tasks = [];

    const processData = function (batch) {
      return function (callback) {
        // Calculate the batch start and end
        const start = batch * batchSize;
        const end = start + batchSize;
        console.log(`Preparing batch ${batch} from ${start} to ${end}`);
        let customers = [];
        // Prepare the customers merged data
        for (i = start; i < end; i++) {
          const customer = Object.assign(customerData[i], customerAdddressData[i]);
          customers.push(customer);
        }
        // Inserting the batch of customers
        db.collection(dbName).insertMany(customers, (error, results) => {
          if (error) return process.exit(1)
          callback(null, results);
        })
      }
    }

    for (i = 0; i < queryCount; i++) {
      tasks.push(processData(i));
    }

    async.parallel(tasks, (error, results) => {
      if (error) console.error(error)
      console.log(`Completed in ${new Date() - startTime}ms`);
      client.close();
      return process.exit();
    })
  });
});
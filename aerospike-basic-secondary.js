"use strict";
const Aerospike = require("aerospike");
const Bluebird = require("bluebird");
const config = {
    hosts: 'localhost:4000'
};

const namespace = process.env.NAMESPACE || 'test';
const set = "test_set_secondary";
const secondaryIndexBin = "test_sec_bin";
const secondaryIndexName = "test_index_sec";
const primaryKey = new Aerospike.Key(namespace, set, 'test_PK');
const secondaryBinValue = "test_secondary_key";
const meta = { ttl: -1 }; // never expire
const record = {
    data: "test data",
    [secondaryIndexBin]: secondaryBinValue
};
const policy = new Aerospike.WritePolicy({
    exists: Aerospike.policy.exists.CREATE_OR_REPLACE
});

const startTime = Date.now();
let totalRequests = 0;

class AerospikeLeakTest {
    async run() {
        await this.initialize();
        setInterval(this.logTestStats, 30 * 1000);

        while (1) {
            await this.findAerospikeSecondary();
            await Bluebird.delay(1);
            totalRequests++;
        }
    }

    logTestStats() {
        const totalTimeSecs = (Date.now() - startTime) / 1000;
        const requestsPerSecond = (totalRequests / totalTimeSecs).toFixed(2);
        console.log(`${new Date().toISOString()} - Stats - requestsPerSecond ${requestsPerSecond}, total: ${totalRequests}, memoryUsage: ${JSON.stringify(process.memoryUsage())}`);
    }

    async findAerospikeSecondary() {
        const query = this.client.query(namespace, set);
        query.where(Aerospike.filter.equal(secondaryIndexBin, secondaryBinValue));
        const stream = query.foreach();

        stream.on('data', function (record) {
            // console.log("data", record);
        });
        stream.on('error', function (error) {
            console.error("error", error);
        });
        stream.on('end', function () {
            // console.log("end");
        });
    }

    async initialize() {
        this.client = await Aerospike.connect(config);
        this.client.put(primaryKey, record, meta, policy);

        try {
            await this.createIndex();
        } catch (err) {
            console.error('Index already exists');
        }
    }

    async createIndex() {
        const options = {
            ns: namespace,
            set,
            bin: secondaryIndexBin,
            index: secondaryIndexName,
            datatype: Aerospike.indexDataType.STRING
        };

        return new Promise(function (resolve, reject) {
            this.client.createIndex(options, function (error, job) {
                if (error) {
                    console.error("error creating index", error);
                    reject();
                    return;
                }

                job.waitUntilDone(function (error) {
                    if (error) {
                        console.error('job error', error);
                        reject();
                        return;
                    }

                    console.log('Index was created successfully.');
                    resolve();
                });
            })
        });
    }
}

const aerospikeLeakTest = new AerospikeLeakTest();
aerospikeLeakTest.run();

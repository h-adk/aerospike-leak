"use strict";
const Aerospike = require("aerospike");
const Bluebird = require("bluebird");
const config = {
    hosts: 'localhost:4000'
};

const namespace = process.env.NAMESPACE || 'test';
const set = "test_set_primary";
const key = new Aerospike.Key(namespace, set, 'test_PK');
const meta = { ttl: -1 }; // never expire
const record = {
    data: "test data"
};
const policy = new Aerospike.WritePolicy({
    exists: Aerospike.policy.exists.CREATE_OR_REPLACE
});

const startTime = Date.now();
let totalRequests = 0;

class AerospikeTest {
    async run() {
        await this.initialize();
        setInterval(this.logTestStats, 30 * 1000);

        while (1) {
            await this.findAerospikePrimary();
            await Bluebird.delay(1);
            totalRequests++;
        }
    }

    logTestStats() {
        const totalTimeSecs = (Date.now() - startTime) / 1000;
        const requestsPerSecond = (totalRequests / totalTimeSecs).toFixed(2);
        console.log(`${new Date().toISOString()} - Stats - requestsPerSecond ${requestsPerSecond}, total: ${totalRequests}, memoryUsage: ${JSON.stringify(process.memoryUsage())}`);
    }

    async findAerospikePrimary() {
        try {
            const data = await this.client.get(key);
            // console.info('found: ', set, key, data);
        }
        catch (err) {
            console.error("error: ", err);
        }
    }

    async initialize() {
        this.client = await Aerospike.connect(config);
        this.client.put(key, record, meta, policy);
    }
}

const aerospikeTest = new AerospikeTest();
aerospikeTest.run();

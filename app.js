#!/usr/bin/env node
const chalk = require("chalk");
const boxen = require("boxen");
const decompress = require('decompress');
const decompressTargz = require('decompress-targz');
const fs = require("fs");
const path = require("path");
const rimraf = require("rimraf");
const dotenv = require('dotenv-safe').config({
    path: '../../rest/.env',
});
const AWS = require('aws-sdk');
const Promise = require('bluebird');
const readFileAsync = Promise.promisify(fs.readFile);
const mongoRestore = require('mongodb-restore-dump');
const { MongoClient } = require("mongodb");

const greeting = chalk.white.bold("Start migration process...");
const boxenOptions = {
 padding: 1,
 margin: 1,
 borderStyle: "round",
 borderColor: "green",
 backgroundColor: "#555555"
};
const msgBox = boxen( greeting, boxenOptions );
console.log(msgBox);
const mongoURL = dotenv.parsed.MONGODB_URI;
console.log(chalk.white.bold("MongoDB URL: " + mongoURL));
const client = new MongoClient(mongoURL);

async function uploadPolicyFilesToS3(policyFiles, organization) {
    const s3 = new AWS.S3({ params: { Bucket: dotenv.parsed.AWS_S3_BUCKET, timeout: 6000000 } });
    let uploadedFiles = [];
    for (const policyFile of policyFiles) {
        try {
        const fileContent = await readFileAsync(policyFile);
        const fileName = path.basename(policyFile);
        const params = {
          Key: `${organization._id}/${fileName}`,
          Body: fileContent
        };
          const stored = await s3.upload(params).promise()
          uploadedFiles.push(stored.Key);
          console.log(chalk.green.bold(`Policy file ${fileName} uploaded to S3`));
        } catch (err) {
          console.log(chalk.white.bold("Upload to s3 error", err));
        }
      }
      console.log(chalk.green.bold('All policy files uploaded to S3'));
      return uploadedFiles;
}

function getListOfPolicyFileInDirectory(dir) {
    let pathDir = `${dir}/s3`;
    let files = fs.readdirSync(pathDir);
    if(files && files[1].includes('org::')) {
        pathDir = `${pathDir}/${files[1]}`;
        files = fs.readdirSync(pathDir).map(file => {
         return path.join(pathDir, file);
        });
        return files
    }
    return [];
}

async function restoreMongoCollections(collections) {
    await dropMongoCollections(collections);
    const destPath = path.resolve(__dirname, "../backupData/dump/dash-rest"); //TODO:: get db name from url
    const uri = dotenv.parsed.MONGODB_URI;
    for (const collection of collections) {
        await mongoRestore.collection({
            uri,
            database: 'dash-rest_restore',
            collection: `back_${collection}`,
            from: `${destPath}/${collection}.bson`,
        });
    }
}

async function dropMongoCollections(collections) {
    console.log(chalk.white.bold("Drop collections: " + collections));
    const mongoClient = await client.connect();
    const db = mongoClient.db("dash-rest_restore");
    for (const collection of collections) {
        try {
            await db.collection(`back_${collection}`).drop();
        } catch (error) {
            if(error.message.match(/ns not found/)) {
                console.log(chalk.white.bold(`Collection back_${collection} not found`));
            } else {
                throw error
            }
        };
    }
    await mongoClient.close();
}

async function copyUsers(organization) {
    const mongoClient = await client.connect();
    const db = mongoClient.db("dash-rest_restore");
    const users = await (await db.collection("back_users").find({}).toArray()).map(user => {
        if(user.organization) {
            user.organization = organization._id;
        }
        return user;
    });
    const usersRestore = await db.collection("users").insertMany(users);
    const organizationAffiliations = await (await db.collection("back_organizationaffiliations").find({}).toArray()).map(item => {
        if(item.organization) {
            item.organization = organization._id;
        }
        return item;
    });
    const organizationAffiliationsRestore = await db.collection("organizationaffiliations").insertMany(organizationAffiliations);
    console.log(chalk.green.bold(`${usersRestore.insertedCount} users restored`));
    console.log(chalk.green.bold(`${organizationAffiliationsRestore.insertedCount} organization affiliations restored`));
    await mongoClient.close();
}

async function copySettings() {
    const mongoClient = await client.connect();
    const db = mongoClient.db("dash-rest_restore");
    const settings = await db.collection("back_settings").find({}).toArray();
    const settingsRestore = await db.collection("settings").insertMany(settings);
    await mongoClient.close();
}

async function copyPolicyCollections(collectionsList) {
    const mongoClient = await client.connect();
    const db = mongoClient.db("dash-rest_restore");
    for (const collection of collectionsList) {
        const collectionRestore = await db.collection(collection).insertMany(await db.collection(collection).find({}).toArray());
    }
    await mongoClient.close();
}

(async ()=>{
    try {
        const fileName = `dump.tar.gz`;
        const filePath = path.resolve(__dirname, "../", fileName);
        const destPath = path.resolve(__dirname, "../backupData");
        console.log(destPath);
        const unzip = await decompress(filePath, destPath, { plugins: [decompressTargz()] });
        console.log(chalk.green.bold("Decompression completed!"));
        console.log(chalk.green.bold("Read data from mongoDB..."));
        const organization = await client.db("dash-rest_restore").collection("organizations").findOne({});
        if(!organization) {
            throw new Error("Organization not found!");
        }
        await client.close();

        const policyFiles = getListOfPolicyFileInDirectory(destPath);
        console.log(chalk.white.bold("Policy Files which will be moved to S3: " + policyFiles));
        // const uploadedPolicyFiles = await uploadPolicyFilesToS3(policyFiles, organization);
        // if(uploadedPolicyFiles.length) {

        // }
        console.log(chalk.white.bold("Restore MongoDb collections..."));
        await restoreMongoCollections(['settings', 'users', 'organizations', 'organizationaffiliations']);
        console.log(chalk.green.bold("Restore MongoDb collections completed!"));
        console.log(chalk.green.bold("Migrate users..."));
        await copyUsers(organization);
        console.log(chalk.green.bold("Migrate users completed!"));
        console.log(chalk.green.bold("Migrate settings..."));
        // await copySettings();
        console.log(chalk.green.bold("Migrate settings completed!"));
        console.log(chalk.green.bold("Migrate policy collections..."));
        console.log(chalk.green.bold("Migrate policy collections completed!"));
    } catch (error) {
        console.log(chalk.red.bold("Decompression failed!"), error);
    } finally {
        // Ensures that the client will close when you finish/error
        await client.close();
        console.log(chalk.green.bold("MongoDB connection closed!"));
    }
})();




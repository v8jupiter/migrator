#!/usr/bin/env node
const chalk = require("chalk");
const boxen = require("boxen");
const decompress = require('decompress');
const decompressTargz = require('decompress-targz');
const fs = require("fs");
const path = require("path");
const rimraf = require("rimraf");
const dotenv = require('dotenv-safe').config({
    path: '../dash-rest/.env',
});
const AWS = require('aws-sdk');
const Promise = require('bluebird');
const readFileAsync = Promise.promisify(fs.readFile);
const mongoRestore = require('mongodb-restore-dump');
const { MongoClient } = require("mongodb");
const yargs = require("yargs");
const axios = require('axios');
const exec = require('node-async-exec');

const mongoURL = dotenv.parsed.MONGODB_URI;
console.log(chalk.white.bold("MongoDB URL: " + mongoURL));
const client = new MongoClient(mongoURL);
const awsS3BucketName = dotenv.parsed.AWS_S3_BUCKET;
const mongoDBName = mongoURL.split('/')[3].split('?')[0];


function showColorBox(msg) {
    const boxenOptions = {
        padding: 1,
        margin: 1,
        borderStyle: "round",
        borderColor: "green",
        backgroundColor: "#555555"
    };
    const msgBox = boxen( msg, boxenOptions );
    console.log(msgBox);
}

function getListOfPolicyFileInDirectory(dir) {
    let pathDir = `${dir}/s3`;
    let files = fs.readdirSync(pathDir);
    if(files && files[1] && files[1].includes('org::')) {
        pathDir = `${pathDir}/${files[1]}`;
        files = fs.readdirSync(pathDir).map(file => {
         return path.join(pathDir, file);
        });
        return files
    }
    return [];
}

async function uploadPolicyFilesToS3(policyFiles, organization) {
    const s3 = new AWS.S3({ params: { Bucket: awsS3BucketName, timeout: 6000000 } });
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

async function dropBackMongoCollections() {
    console.log(chalk.white.bold("Drop back_ collections: "));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    const collections = await db.listCollections().toArray();
    for (const collection of collections) {
        try {
            if(collection.name.includes("back_")) {
                console.log(chalk.white.bold("Drop collections: " + collection.name));
                await db.collection(collection.name).drop();
            }
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

async function movePolicyFiles(destPath, organization) {
    const policyFiles = getListOfPolicyFileInDirectory(destPath);
    console.log(chalk.white.bold("Policy Files which will be moved to S3: " + policyFiles));
    const policyFilesUploaded = await uploadPolicyFilesToS3(policyFiles, organization);
    console.log(chalk.green.bold("Policy Files uploaded to S3"));
    return policyFilesUploaded;
}

async function backupAllCollections() {
    console.log(chalk.green.bold("Backup collections..."));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    const collections = await db.listCollections().toArray();
    for (const collection of collections) {
        if(!collection.name.includes("back_")) {
            await db.collection(collection.name).rename(`back_${collection.name}`);
        }
    }
    await mongoClient.close();
    console.log(chalk.green.bold("Backup collections completed!"));
}

function getMongoDumpFiles(dir) {
    let pathDir = `${dir}/dump/dash-rest`;
    let files = fs.readdirSync(pathDir);
    files = fs.readdirSync(pathDir).map(file => {
         return path.join(pathDir, file);
        }).filter(file => { return path.extname(file) === ".bson"; });
    return files
}

async function restoreAllMongoCollections(destPath) {
    console.log(chalk.green.bold("Restore collections..."));
    const bsonFiles = getMongoDumpFiles(destPath);
    const uri = dotenv.parsed.MONGODB_URI;
    for (const bsonFile of bsonFiles) {
        const collectionName = path.basename(bsonFile, ".bson");
        await mongoRestore.collection({
            uri,
            database: mongoDBName,
            collection: collectionName,
            from: bsonFile,
        });
    }

    console.log(chalk.green.bold("Restore collections completed!"));
}

async function getCurrentOrganization() {
    console.log(chalk.green.bold("Read organization data from mongoDB..."));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    const organization = await db.collection("organizations").findOne({});
    if(!organization) {
        throw new Error("Organization not found!");
    }
    await client.close();
    console.log(chalk.white.bold(`Organization ${organization} found`));
    return organization;
}

function getOrganizationReplacementRules() {
    return [
        {
            "collection": "organizations",
            "field": "_code",
        },
        {
            "collection": "organizations",
            "field": "_coreId",
        },
        {
            "collection": "organizations",
            "field": "_dashApiSecret",
        },
        {
            "collection": "organizations",
            "field": "orgCoreToken",
        }
    ];
}

async function updateOrganizationInMongoCollection(organization) {
    console.log(chalk.green.bold("Update organization data in mongoDB..."));
    const rules = getOrganizationReplacementRules();
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    for (const rule of rules) {
        const collection = db.collection(rule.collection);
        const updateResult = await collection.updateMany({}, { $set: { [rule.field]: organization[rule.field] } });
        console.log(chalk.white.bold(`${updateResult.modifiedCount} documents updated in ${rule.collection}`));
    }
    await mongoClient.close();
    console.log(chalk.green.bold("Update organization data in mongoDB completed!"));
}   

async function updateBucketForPolicyFilesInMongoCollection(bucketName) {
    console.log(chalk.green.bold("Update bucket name in mongoDB..."));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    await db.collection("files").updateMany({}, { $set: { bucket: bucketName } });
    await mongoClient.close();
    console.log(chalk.green.bold("Update bucket name in mongoDB completed!"));
}

async function downloadBackup(s3link, fileName) {
    console.log(chalk.white.bold("Download backup..."));
    const filePath = `${__dirname}/../${fileName}`;
    const file = fs.createWriteStream(filePath);
    const response = await axios.get(s3link, { responseType: 'stream' });
    response.data.pipe(file);
    return new Promise((resolve, reject) => {
        file.on('finish', () => {
            file.close();
            console.log(chalk.green.bold("Download backup completed!"));
            resolve(filePath);
        });
        file.on('error', err => {
            fs.unlink(filePath);
            reject(err);
        });
    });
}

async function replaceTemplateUrlInMongoSettings() {
    console.log(chalk.green.bold("Update template url in mongoDB..."));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    const templateUrl = db.collection(`back_settings`).findOne({dashSettingKey: "templateUrl"});
    await db.collection("settings").updateOne({dashSettingKey: "templateUrl"}, { $set: { value: templateUrl } });
    await mongoClient.close();
    console.log(chalk.green.bold("Update template url in mongoDB completed!"));
}

async function removeSSLDomainFromMongoSettings() {
    console.log(chalk.green.bold("Remove SSL domain from mongoDB..."));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    await db.collection("settings").deleteOne({dashSettingKey: "sslDomain"});
    await mongoClient.close();
    console.log(chalk.green.bold("Remove SSL domain from mongoDB completed!"));
}

async function disconnectAllAWSaccounts() {
    console.log(chalk.green.bold("Disconnect all AWS accounts..."));
    const mongoClient = await client.connect();
    const db = mongoClient.db(mongoDBName);
    await db.collection("awsaccounts").updateMany({}, { $set: { isValidated: false } }); 
    mongoClient.close();
    console.log(chalk.green.bold("Disconnect all AWS accounts completed!"));
}

async function startStopPm2(action) {
    console.log(chalk.white.bold(`${action} PM2...`));
    await exec({ cmd: `pm2 ${action}` });
    console.log(chalk.green.bold(`${action} PM2 completed!`));
}

async function runMongoMigrations() {
    console.log(chalk.green.bold("Run mongo migrations..."));
    await exec({ 
        path: `${__dirname}/../dash-rest`,
        cmd: `npm run migrate` 
    });
    console.log(chalk.green.bold("Run mongo migrations completed!"));
}

(async ()=>{
    try {
        const options = yargs
            .usage("Usage: -u <s3_public_url>")
            .option("u", { alias: "s3url", describe: "Public S3 URL for tar.gz dump file", type: "string", demandOption: true })
            .check(argv => {
                if(!argv.s3url) {
                    throw new Error("URL is required!");
                }
                if(!argv.s3url.match(/^https:\/\/dash-backup-[a-zA-Z0-9-]+\.s3\.amazonaws\.com\/[a-zA-Z0-9-_]+\.tar\.gz$/)) {
                    throw new Error("URL is not S3 valid!");
                } else {
                    return true;
                }
            })
            .boolean("local")
            .argv;

        console.log(options.d? "Dump mode": "Restore mode");
        showColorBox("Start migration process...")
        if(!options.local) {
            await startStopPm2("stop all");
        }
        const fileName = `dump.tar.gz`;
        await downloadBackup(options.s3url, fileName);
        const filePath = path.resolve(__dirname, "../", fileName);
        const destPath = path.resolve(__dirname, "../backupData");
        console.log(destPath);
        const unzip = await decompress(filePath, destPath, { plugins: [decompressTargz()] });
        console.log(chalk.green.bold("Decompression completed!"));
        const newOrganization = await getCurrentOrganization();
        await dropBackMongoCollections();
        await backupAllCollections();
        await restoreAllMongoCollections(destPath);
        const dumpedORganization = await getCurrentOrganization();
        await updateOrganizationInMongoCollection(newOrganization);
        await updateBucketForPolicyFilesInMongoCollection(awsS3BucketName);
        await movePolicyFiles(destPath, dumpedORganization);
        await replaceTemplateUrlInMongoSettings();
        await removeSSLDomainFromMongoSettings();
        await disconnectAllAWSaccounts();
        
        if(!options.local) {
            await runMongoMigrations();
            await startStopPm2("start all");
            await startStopPm2("status")
        }
        showColorBox("Migration completed!")
    } catch (error) {
        console.log(chalk.red.bold("Migration failed! ERROR:"), error);
    }
})();
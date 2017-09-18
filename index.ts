import * as mongodb from 'mongodb';

/**
 * confi.private example
 *
 * export const map_file = './der2_Refset_ICD10CM_US_20170301/der2_iisssccRefset_ExtendedMapSnapshot_US1000124_20170301.txt';
 * export const collection_name = 'snomed-cie10';
 * export const mongoHost = 'mongodb://localhost:27017/andes';
 */
import * as config from './config.private';


/**
 * Iniciamos la conexión con mongo y borramos lo que allá en la colleción
 */
mongodb.MongoClient.connect(config.mongoHost, function (err: any, db) {
    if (err) {
        console.log(err);
        process.exit();
        return;
    }
    db.collection(config.collection_name).remove().then((result) => {
        console.log(result.result);
        init(db);

    });
});

let toSave = [];

/**
 * Leemos el archivo linea por linea e insertamos en la collecion de destino
 *
 * @param {any} db
 */
function init(db) {
    var lineByLine = require('n-readlines');
    var liner = new lineByLine(config.map_file);

    var line;
    var lineNumber = 0;
    while (line = liner.next()) {
        readLine(db, line.toString('ascii'));
    }
    saveValue(db);
    console.log('Terminamos!');
    // process.exit();
}

/**
 * Lee una linea y arma un objeto de mapeo
 *
 * @param {any} db
 * @param {any} line
 */

function readLine(db, line) {
    let columns = line.split('\t');
    if (columns[0] !== 'id' && columns.length > 2) {
        let data = {
            conceptId: columns[5],
            mapGroup: columns[6],
            mapPriority: columns[7],
            mapRule: parseRules(columns[8]),
            mapAdvice: columns[9],
            mapTarget: columns[10]
        };
        toSave.push(data);
        if (toSave.length >= 5000) {
            saveValue(db);
        }
    }
}

/**
 * Graba un array de items en mongo
 * @param {any} db
 */
function saveValue(db) {

    let p = db.collection(config.collection_name).insertMany(toSave);
    toSave = [];

    var waitTill = new Date(new Date().getTime() + 200);
    while (waitTill > new Date()) { }

}

/**
 * Parsea una regla de mapeo y la convierte en un objecto JSON
 *
 * @param {any} rules
 * @returns
 */

function parseRules(rules) {
    if (rules === 'TRUE') {
        return [true];
    } else if (rules === 'OTHERWISE TRUE') {
        return [false];
    } else {
        let q;
        let exp = [];
        try {
            while (rules.length) {
                q = readExp(rules);
                exp.push({ concept: q.concept, value: q.value });
                rules = q.rules;
            }
        } catch (e) {
            console.log(rules);
            process.exit();
        }
        return exp;
    }
}

/**
 * Funcion recursiva para leer Las expresiones IFA AND de las reglas de mapeo
 *
 * @param {String} rules
 * @returns
 */

function readExp(rules: String) {
    rules = rules.substr(4);
    let space = rules.indexOf(' ');

    let concepID = rules.substr(0, space);

    let pipe = rules.indexOf('|');
    let secondPipe = rules.indexOf('|', pipe + 1);

    rules = rules.substr(secondPipe + 1).trim();

    if (rules.startsWith('AND')) {
        return {
            rules: rules.substr(4),
            concept: concepID,
            value: null
        };
    } else if (!rules.length) {
        return {
            rules: '',
            concept: concepID,
            value: null
        };
    } else {
        let split = rules.split(' ');
        return {
            rules: '',
            concept: concepID,
            value: {
                op: split[0],
                number: split[1],
                unit: split[2],
            }
        };
    }

}

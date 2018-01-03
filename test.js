const fortune = require('fortune');

const adapter = require('./');

const typeMap = require('../api/src/type-map');
const typeDefinitions = require('../api/src/type-definitions');

const options = {};

options.adapter = [
    adapter,
    {
        typeMap: typeMap,
        databaseUrl: 'progres://laborsync:development@localhost:5432/data1'
    }
];

const store = fortune(typeDefinitions, options);

let findIds;
let findOptions;

// findIds = '1nQjQikoqsCHx6Mv';

findOptions = {
    match: {
        email: 'glogan@crscompany.net'
    },
};

// store.find('user', findIds, findOptions)
//     .then((results) => {
//         console.log(results.payload.records);
//     })
//     .catch(console.error);

// store.create('token', [{
//         account: 'EzYuN3UI21aOrPzU',
//         user: 'Zgo85nn2TJrZdJa9',
//         ip: '192.168.1.23',
//         status: 'active'
//     }]).then((results) => {
//         console.log(results.payload);
//     })
//     .catch(console.error);

// store.update('token', [{
//         id: 'WXRvRF4cnQhaPRok',
//         replace: {
//             status: 'test',
//             // updated: '2019-01-04 20:30:34.064165'
//         }
//     }]).then((results) => {
//         console.log(results.payload);
//     })
//     .catch(console.error);

store.delete('token', [
        'WXRvRF4cnQhaPRok'
    ]).then((results) => {
        console.log(results.payload);
    })
    .catch(console.error);
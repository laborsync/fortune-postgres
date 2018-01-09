const fortune = require('fortune');

const adapter = require('./');

const typeMap = require('../api/src/type-map');
const typeDefinitions = require('../api/src/type-definitions');

const options = {};

options.adapter = [
    adapter,
    {
        typeMap: typeMap,
        url: 'progres://laborsync:development@localhost:5432/data1'
    }
];


const store = fortune(typeDefinitions, options);

let findIds;
let findOptions;

// // findIds = '1nQjQikoqsCHx6Mv';

// findOptions = {
//     match: {
//         email: 'glogan@crscompany.net'
//     },
// };

// store.find('user', findIds, findOptions)
//     .then((results) => {
//         console.log(results.payload);
//     })
//     .catch(console.error);

// store.create('token', [{
//         account: 'EzYuN3UI21aOrPzU',
//         created: this.now,
//         user: 'Zgo85nn2TJrZdJa9',
//         ip: '192.168.1.23',
//         status: 'active',
//         created: '2018-01-08 20:57:09.977505',
//         updated: '2018-01-08 20:57:09.977505'
//     }]).then((results) => {
//         console.log(results.payload);
//     })
//     .catch(console.error);

// store.update('token', [{
//         id: 'msfpNksBZkspiibe',
//         replace: {
//             status: 'test',
//             updated: '2018-01-08 21:57:09.977505',
//         }
//     }]).then((results) => {
//         console.log(results.payload);
//     })
//     .catch(console.error);

// store.delete('token', [
//         'msfpNksBZkspiibe'
//     ]).then((results) => {
//         console.log(results.payload);
//     })
//     .catch(console.error);

// store.adapter.now()
//     .then((now) => {
//         console.log(now);
//     })
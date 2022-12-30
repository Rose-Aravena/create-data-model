require('dotenv').config();

module.exports = {
client: 'oracledb',
    connection: {
        user: process.env.USER,
        password: process.env.PASSWORD,
        connectString: process.env.CONNECT_STRING
    }
};

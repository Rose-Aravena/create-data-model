const knexConfig = require('./db.config');
const knex = require('knex')(knexConfig);
const parse = require('csv-parse').parse;
const fs = require('fs');

const getAllSubtablesFiles = async () => {
    const files = await fs.promises.readdir('./tables/subtables');
    parsingCsvSubTables(files)
}

const parsingCsvSubTables = async (files) => {
    const lettersReg = /(^$)|[a-zA-Z]/
    files.forEach(async file => {
        const data = await fs.promises.readFile(`./tables/subtables/${file}`, 'utf-8');
        parse(data, {delimiter: ',', columns:true, skip_empty_lines: true}, async (err, data) => {
            if (err) {
                console.error(err.message)
            }
            for (let i = 0; i < data.length; i++){
                for(const [key, value] of Object.entries(data[i])){
                    const newKey = key.normalize('NFD').replace(/[\u0300-\u036f]/g,"").toUpperCase().replace(/ /gi, '_');
                    data[i][newKey] = value;
                    delete data[i][key];
                } 
            }
            data.forEach(row => {
                if(Object.values(row)[0].match(lettersReg)){
                    data.type = 'string'
                }else{
                    data.type = 'integer'
                }
            });
            const headers = Object.keys(data[0]);
            let newstr = file.replace(/.csv/gi, '');
            await createSubTable(newstr, headers, data.type);
            await insertData(newstr, data);
            //await dropTable(newstr)
        });
    })
 };

const createSubTable = async (tableName, columns, dataType) => {
    try{
       await knex.schema.createTable(tableName, table => {
        dataType === 'string' ? table.string(columns[0]).primary() : table.integer(columns[0]).primary(); 
        for (i=1; i < columns.length; i++){
            table.string(columns[i])
        }
        })
        console.info('Table created');
    }catch(err) {
        console.error(err.message)
    }finally{
        knex.destroy()
    }
}

const insertData = async (tableName, data) => {
    try{
        await knex(tableName).insert(data);
        console.info('Data inserted')
    }catch(err){
        console.error(err.message)
    }finally{
        knex.destroy()
    }
}

const dropTable = async (tableName) => {
    try{
        await knex.schema.dropTable(tableName);
        console.info('Table droped')
    }catch(err){
        console.error(err.message)
    }finally{
        knex.destroy()
    }
}

const viewInfo = async () => {
    try{
        await knex.select().from('SUB_OPCIONES_DIAMETRO').then((info) => {
            console.info(info);
        });
    }catch(err){
        console.error(err.message)
    }finally{
        knex.destroy()
    }
};

viewInfo()
//getAllSubtablesFiles();
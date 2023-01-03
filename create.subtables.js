const knexConfig = require('./db.config');
const knex = require('knex')(knexConfig);
const parse = require('csv-parse').parse;
const fs = require('fs');

let mainTable;

const getAllSubtablesFiles = async () => {
    const files = await fs.promises.readdir('./tables/subtables');
    const mainTableData = await knex('GIS37_GEOGOTA_GIS38').columnInfo();
    mainTable = Object.keys(mainTableData);
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
                    const newValue = value.normalize('NFD').replace(/[\u0300-\u036f]/g,"").trim();
                    data[i][newKey] = newValue;
                    delete data[i][key];
                }   
            };
            const headers = Object.keys(data[0]);
            let tableName = file.replace(/.csv/gi, '');
            await createSubTable(tableName, headers);
            await insertData(tableName, data);
            //await dropTable(tableName)
        });
    })
 };

const createSubTable = async (tableName, columns) => {
    try{
       await knex.schema.createTable(tableName, table => {
            if(tableName === 'SUB_COD_CUADRILLA'){
                table.string(columns[0]);
            }else if(tableName === 'SUB_COD_EMPRESA'){
                table.integer(columns[0]).primary();
            }else{
                /* dataType === 'string' ? */ table.string(columns[0]).primary() /* : table.integer(columns[0]).primary();  */
            }
            for (i=1; i < columns.length; i++){
                table.string(columns[i])
            }
        })
        console.info('Table created');
    }catch(err) {
        console.error(err.message)
    }
}

const insertData = async (tableName, data) => {
    try{
        await knex(tableName).insert(data);
        console.info('Data inserted')
    }catch(err){
        console.error(err.message)
    }
}

const dropTable = async (tableName) => {
    try{
        await knex.schema.dropTable(tableName);
        console.info('Table droped')
    }catch(err){
        console.error(err.message)
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

//viewInfo()
getAllSubtablesFiles();

/*data.forEach(row => {
    if(Object.values(row)[0].match(lettersReg)){
        data.type = 'string'
    }else{
        data.type = 'integer'
    }
}); */

/*if(tableName.includes(mainTable[i])){
    console.log(tableName, mainTable[i])
} */